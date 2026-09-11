//go:build storageffi

package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nickemma/meridian/internal/config"
	"github.com/nickemma/meridian/internal/consistency"
	kv "github.com/nickemma/meridian/proto/kv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type testNodeAddress struct {
	raftPort   int
	clientPort int
}

func TestStrongKVOverLiveThreeNodeCluster(t *testing.T) {
	addresses := make([]testNodeAddress, 3)
	for index := range addresses {
		addresses[index] = testNodeAddress{raftPort: freePort(t), clientPort: freePort(t)}
	}
	dataDirs := make([]string, len(addresses))
	for index := range dataDirs {
		dataDirs[index] = t.TempDir()
	}
	ctx, cancel := context.WithCancel(context.Background())
	var serverWG sync.WaitGroup
	nodeCancels := make([]context.CancelFunc, len(addresses))
	nodeDone := make([]chan struct{}, len(addresses))
	configs := make([]*config.Config, len(addresses))
	t.Cleanup(func() {
		cancel()
		serverWG.Wait()
	})

	servers := make([]*Server, 0, len(addresses))
	for index, address := range addresses {
		peers := make([]config.Peer, 0, len(addresses)-1)
		for peerIndex, peerAddress := range addresses {
			if peerIndex == index {
				continue
			}
			peers = append(peers, config.Peer{ID: fmt.Sprintf("node-%d", peerIndex+1), Address: fmt.Sprintf("127.0.0.1:%d", peerAddress.raftPort)})
		}
		cfg := &config.Config{
			NodeID:             fmt.Sprintf("node-%d", index+1),
			RaftPort:           address.raftPort,
			ClientPort:         address.clientPort,
			DataDir:            dataDirs[index],
			Peers:              peers,
			QuorumSize:         2,
			ElectionTimeoutMin: 500 * time.Millisecond,
			ElectionTimeoutMax: 750 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
			Policies: []consistency.NamespacePolicy{
				{Prefix: "/strong/", Class: consistency.Strong, Version: 1},
				{Prefix: "/causal/", Class: consistency.Causal, Version: 2},
				{Prefix: "/eventual/", Class: consistency.Eventual, Version: 3},
			},
		}
		configs[index] = cfg
		server, err := New(cfg)
		if err != nil {
			t.Fatalf("create node %d: %v", index+1, err)
		}
		servers = append(servers, server)
		nodeCtx, nodeCancel := context.WithCancel(ctx)
		nodeCancels[index] = nodeCancel
		nodeDone[index] = make(chan struct{})
		serverWG.Add(1)
		go func(server *Server, nodeCtx context.Context, done chan<- struct{}) {
			defer serverWG.Done()
			defer close(done)
			if err := server.Start(nodeCtx); err != nil && nodeCtx.Err() == nil {
				t.Errorf("run server: %v", err)
			}
		}(server, nodeCtx, nodeDone[index])
	}

	client, closeClient := waitForLeaderClient(t, ctx, addresses)
	defer closeClient()
	operationCtx, operationCancel := context.WithTimeout(ctx, 5*time.Second)
	defer operationCancel()
	put, err := client.Put(operationCtx, &kv.PutRequest{RequestId: "put-1", Key: []byte("/strong/item"), Value: []byte("value"), Consistency: kv.Consistency_CONSISTENCY_STRONG, Context: &kv.CausalContext{PolicyVersion: 1}})
	if err != nil {
		t.Fatalf("strong put: %v", err)
	}
	if put.RaftIndex == 0 {
		t.Fatal("strong put returned zero Raft index")
	}
	get, err := client.Get(operationCtx, &kv.GetRequest{RequestId: "get-1", Key: []byte("/strong/item"), Consistency: kv.Consistency_CONSISTENCY_STRONG})
	if err != nil {
		t.Fatalf("strong get: %v", err)
	}
	if !get.Found || string(get.Value) != "value" || get.RaftIndex < put.RaftIndex {
		t.Fatalf("strong get = found:%t value:%q index:%d, put index:%d", get.Found, get.Value, get.RaftIndex, put.RaftIndex)
	}
	causalStrong, err := client.Get(operationCtx, &kv.GetRequest{RequestId: "causal-strong-get", Key: []byte("/strong/item"), Consistency: kv.Consistency_CONSISTENCY_CAUSAL})
	if err != nil {
		t.Fatalf("causal read of materialized strong value: %v", err)
	}
	if !causalStrong.Found || string(causalStrong.Value) != "value" || causalStrong.ServedConsistency != kv.Consistency_CONSISTENCY_CAUSAL {
		t.Fatalf("causal strong read = %#v", causalStrong)
	}
	eventualStrong, err := client.Get(operationCtx, &kv.GetRequest{RequestId: "eventual-strong-get", Key: []byte("/strong/item"), Consistency: kv.Consistency_CONSISTENCY_EVENTUAL})
	if err != nil {
		t.Fatalf("eventual read of materialized strong value: %v", err)
	}
	if !eventualStrong.Found || string(eventualStrong.Value) != "value" || eventualStrong.ServedConsistency != kv.Consistency_CONSISTENCY_EVENTUAL {
		t.Fatalf("eventual strong read = %#v", eventualStrong)
	}
	strongMaterialized := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		causalView, causalErr := servers[1].causal.Read([]byte("/strong/item"), nil, 0)
		eventualView := servers[2].eventual.Read([]byte("/strong/item"))
		if causalErr == nil && len(causalView.Values) == 1 && string(causalView.Values[0].Value) == "value" && len(eventualView.Values) == 1 && string(eventualView.Values[0].Value) == "value" {
			strongMaterialized = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strongMaterialized {
		t.Fatal("strong version was not materialized on follower weak paths")
	}

	causalPut, err := client.Put(operationCtx, &kv.PutRequest{RequestId: "causal-put-1", Key: []byte("/causal/item"), Value: []byte("causal-value"), Consistency: kv.Consistency_CONSISTENCY_CAUSAL, Context: &kv.CausalContext{PolicyVersion: 2}})
	if err != nil {
		t.Fatalf("causal put: %v", err)
	}
	if causalPut.GetContext().GetPolicyVersion() != 2 || len(causalPut.GetContext().GetVectorClock()) == 0 {
		t.Fatalf("causal put response lacks context: %#v", causalPut)
	}
	causalGet, err := client.Get(operationCtx, &kv.GetRequest{RequestId: "causal-get-1", Key: []byte("/causal/item"), Consistency: kv.Consistency_CONSISTENCY_CAUSAL, Context: causalPut.GetContext()})
	if err != nil {
		t.Fatalf("causal get: %v", err)
	}
	if !causalGet.Found || string(causalGet.Value) != "causal-value" || len(causalGet.Versions) != 1 {
		t.Fatalf("causal get = %#v", causalGet)
	}

	deadline = time.Now().Add(2 * time.Second)
	causalReplicated := false
	for time.Now().Before(deadline) {
		remote, err := servers[1].causal.Read([]byte("/causal/item"), nil, 0)
		if err == nil && len(remote.Values) == 1 && string(remote.Values[0].Value) == "causal-value" {
			causalReplicated = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !causalReplicated {
		t.Fatal("causal update was not disseminated to peer")
	}

	eventualPut, err := client.Put(operationCtx, &kv.PutRequest{RequestId: "eventual-put-1", Key: []byte("/eventual/item"), Value: []byte("eventual-value"), Consistency: kv.Consistency_CONSISTENCY_EVENTUAL, Context: &kv.CausalContext{PolicyVersion: 3}})
	if err != nil {
		t.Fatalf("eventual put: %v", err)
	}
	if eventualPut.GetContext().GetPolicyVersion() != 3 || len(eventualPut.GetContext().GetVectorClock()) == 0 {
		t.Fatalf("eventual put response lacks context: %#v", eventualPut)
	}
	eventualGet, err := client.Get(operationCtx, &kv.GetRequest{RequestId: "eventual-get-1", Key: []byte("/eventual/item"), Consistency: kv.Consistency_CONSISTENCY_EVENTUAL, Context: eventualPut.GetContext()})
	if err != nil {
		t.Fatalf("eventual get: %v", err)
	}
	if !eventualGet.Found || string(eventualGet.Value) != "eventual-value" || len(eventualGet.Versions) != 1 {
		t.Fatalf("eventual get = %#v", eventualGet)
	}

	deadline = time.Now().Add(2 * time.Second)
	eventualReplicated := false
	for time.Now().Before(deadline) {
		remote := servers[2].eventual.Read([]byte("/eventual/item"))
		if len(remote.Values) == 1 && string(remote.Values[0].Value) == "eventual-value" {
			eventualReplicated = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !eventualReplicated {
		t.Fatal("eventual update was not disseminated to peer")
	}

	leaderIndex := -1
	for index, server := range servers {
		if server.RaftNode().Status().Role == "Leader" {
			leaderIndex = index
			break
		}
	}
	if leaderIndex < 0 {
		t.Fatal("could not identify leader before failure")
	}
	nodeCancels[leaderIndex]()
	select {
	case <-nodeDone[leaderIndex]:
	case <-time.After(3 * time.Second):
		t.Fatal("leader did not stop")
	}

	postFailureClient, closePostFailureClient := waitForLeaderClient(t, ctx, addresses)
	defer closePostFailureClient()
	postFailureCtx, postFailureCancel := context.WithTimeout(ctx, 5*time.Second)
	defer postFailureCancel()
	postFailurePut, err := postFailureClient.Put(postFailureCtx, &kv.PutRequest{RequestId: "post-failure-put", Key: []byte("/strong/after-failure"), Value: []byte("recovered"), Consistency: kv.Consistency_CONSISTENCY_STRONG, Context: &kv.CausalContext{PolicyVersion: 1}})
	if err != nil || postFailurePut.RaftIndex == 0 {
		t.Fatalf("strong put after leader failure = (%#v, %v)", postFailurePut, err)
	}

	restarted, err := New(configs[leaderIndex])
	if err != nil {
		t.Fatalf("reopen stopped leader: %v", err)
	}
	servers[leaderIndex] = restarted
	restartedCtx, restartedCancel := context.WithCancel(ctx)
	nodeCancels[leaderIndex] = restartedCancel
	nodeDone[leaderIndex] = make(chan struct{})
	serverWG.Add(1)
	go func() {
		defer serverWG.Done()
		defer close(nodeDone[leaderIndex])
		if err := restarted.Start(restartedCtx); err != nil && restartedCtx.Err() == nil {
			t.Errorf("restart leader: %v", err)
		}
	}()

	deadline = time.Now().Add(5 * time.Second)
	restartedCaughtUp := false
	for time.Now().Before(deadline) {
		read, err := postFailureClient.Get(postFailureCtx, &kv.GetRequest{RequestId: "post-restart-get", Key: []byte("/strong/after-failure"), Consistency: kv.Consistency_CONSISTENCY_STRONG})
		initial, initialFound, initialErr := restarted.store.Get([]byte("/strong/item"))
		recovered, recoveredFound, recoveredErr := restarted.store.Get([]byte("/strong/after-failure"))
		if err == nil && read.Found && string(read.Value) == "recovered" && initialErr == nil && initialFound && string(initial) == "value" && recoveredErr == nil && recoveredFound && string(recovered) == "recovered" {
			restartedCaughtUp = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !restartedCaughtUp {
		t.Fatal("restarted former leader did not catch up with acknowledged strong values")
	}
}

func waitForLeaderClient(t *testing.T, ctx context.Context, addresses []testNodeAddress) (kv.KVServiceClient, func()) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	const leaderStability = 300 * time.Millisecond
	for time.Now().Before(deadline) {
		for _, address := range addresses {
			dialCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			connection, err := grpc.DialContext(dialCtx, fmt.Sprintf("127.0.0.1:%d", address.clientPort), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
			cancel()
			if err != nil {
				continue
			}
			client := kv.NewKVServiceClient(connection)
			statusCtx, statusCancel := context.WithTimeout(ctx, 200*time.Millisecond)
			response, err := client.Status(statusCtx, &kv.StatusRequest{})
			statusCancel()
			if err == nil && response.Role == "Leader" {
				// A peer can report leader while another candidate that started
				// before its first heartbeat is still completing an election.
				// Require the same endpoint to retain leadership briefly before
				// issuing the test workload.
				select {
				case <-ctx.Done():
					_ = connection.Close()
					return nil, func() {}
				case <-time.After(leaderStability):
				}
				confirmCtx, confirmCancel := context.WithTimeout(ctx, 200*time.Millisecond)
				confirmed, confirmErr := client.Status(confirmCtx, &kv.StatusRequest{})
				confirmCancel()
				if confirmErr == nil && confirmed.Role == "Leader" {
					return client, func() { _ = connection.Close() }
				}
			}
			_ = connection.Close()
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("cluster did not elect a leader")
	return nil, nil
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}
