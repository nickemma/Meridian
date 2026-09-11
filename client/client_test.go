package client

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	kv "github.com/nickemma/meridian/proto/kv"
	"google.golang.org/grpc"
)

type testService struct {
	kv.UnimplementedKVServiceServer
	role string
	puts atomic.Uint64
}

func (s *testService) Status(context.Context, *kv.StatusRequest) (*kv.StatusResponse, error) {
	return &kv.StatusResponse{Role: s.role}, nil
}

func (s *testService) Put(context.Context, *kv.PutRequest) (*kv.PutResponse, error) {
	s.puts.Add(1)
	return &kv.PutResponse{ServedConsistency: kv.Consistency_CONSISTENCY_STRONG}, nil
}

func TestStrongPutUsesDiscoveredLeader(t *testing.T) {
	followerAddress, follower, stopFollower := serve(t, "Follower")
	defer stopFollower()
	leaderAddress, leader, stopLeader := serve(t, "Leader")
	defer stopLeader()
	client, err := New([]string{followerAddress, leaderAddress})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Put(context.Background(), &kv.PutRequest{Consistency: kv.Consistency_CONSISTENCY_STRONG}); err != nil {
		t.Fatal(err)
	}
	if follower.puts.Load() != 0 || leader.puts.Load() != 1 {
		t.Fatalf("puts follower=%d leader=%d", follower.puts.Load(), leader.puts.Load())
	}
}

func serve(t *testing.T, role string) (string, *testService, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	service := &testService{role: role}
	kv.RegisterKVServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	return listener.Addr().String(), service, func() {
		server.Stop()
		_ = listener.Close()
	}
}

func TestNewRejectsEmptyTargets(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("New accepted an empty target list")
	}
}
