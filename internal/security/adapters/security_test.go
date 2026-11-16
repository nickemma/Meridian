package adapters

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	kv "github.com/nickemma/meridian/proto/kv"
	raftpb "github.com/nickemma/meridian/proto/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/nickemma/meridian/internal/security/domain"
)

func fixture(t *testing.T) (string, *Bundle) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "pki")
	if err := GenerateDevelopmentPKI(dir); err != nil {
		t.Fatal(err)
	}
	b, err := Load(nodeConfig(dir, "node-1"), "node-1", []string{"node-2", "node-3"})
	if err != nil {
		t.Fatal(err)
	}
	return dir, b
}

// nodeConfig names the development PKI files for one node.
func nodeConfig(dir, node string) domain.Config {
	return domain.Config{CAFile: filepath.Join(dir, "ca.pem"), CertFile: filepath.Join(dir, node+".pem"), KeyFile: filepath.Join(dir, node+".key"), ClientsFile: filepath.Join(dir, "clients.json")}
}

func identityContext(t *testing.T, dir, id string) context.Context {
	t.Helper()
	pair, err := tls.LoadX509KeyPair(filepath.Join(dir, id+".pem"), filepath.Join(dir, id+".key"))
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}}})
}
func TestTenantAndPermissionMatrix(t *testing.T) {
	dir, b := fixture(t)
	for _, class := range []kv.Consistency{kv.Consistency_CONSISTENCY_STRONG, kv.Consistency_CONSISTENCY_CAUSAL, kv.Consistency_CONSISTENCY_EVENTUAL} {
		for _, id := range []string{"tenant-a", "tenant-a-reader", "tenant-b", "unregistered"} {
			for _, key := range []string{"/tenants/a/strong/item", "/tenants/b/strong/item", "/tenants/ab/strong/item", "/tenants/a/../b/strong/item", "/tenants/a//item", "/tenants/a/item\x00", "/tenants/a/\\item"} {
				for _, op := range []string{"get", "put", "delete", "cas"} {
					t.Run(id+"/"+op+"/"+class.String()+"/"+key, func(t *testing.T) {
						var req any
						switch op {
						case "get":
							req = &kv.GetRequest{Key: []byte(key), Consistency: class}
						case "put":
							req = &kv.PutRequest{Key: []byte(key), Consistency: class}
						case "delete":
							req = &kv.DeleteRequest{Key: []byte(key), Consistency: class}
						case "cas":
							req = &kv.CompareAndSetRequest{Key: []byte(key), Consistency: class}
						}
						allowed := (id == "tenant-a" && key == "/tenants/a/strong/item") || (id == "tenant-a-reader" && key == "/tenants/a/strong/item" && op == "get") || (id == "tenant-b" && key == "/tenants/b/strong/item")
						calls := 0
						_, err := b.authorizeClient(identityContext(t, dir, id), req, nil, func(context.Context, any) (any, error) { calls++; return nil, nil })
						if allowed {
							if err != nil || calls != 1 {
								t.Fatalf("authorized request blocked: %v", err)
							}
						} else {
							if err == nil || calls != 0 {
								t.Fatalf("unauthorized handler call: %v", err)
							}
						}
					})
				}
			}
		}
	}
}
func TestRaftSenderBinding(t *testing.T) {
	dir, b := fixture(t)
	ctx := identityContext(t, dir, "node-2")
	for _, id := range []string{"node-2", "node-3", ""} {
		for _, req := range []any{&raftpb.RequestVoteRequest{CandidateId: id}, &raftpb.PreVoteRequest{CandidateId: id}, &raftpb.AppendEntriesRequest{LeaderId: id}} {
			calls := 0
			_, err := b.authorizePeer(ctx, req, nil, func(context.Context, any) (any, error) { calls++; return nil, nil })
			if id == "node-2" {
				if err != nil || calls != 1 {
					t.Fatal(err)
				}
			} else if status.Code(err) != codes.PermissionDenied || calls != 0 {
				t.Fatalf("spoofed Raft sender reached handler: %v", err)
			}
		}
	}
}

type receiver struct {
	kv.UnimplementedKVServiceServer
	raftpb.UnimplementedRaftServiceServer
	calls atomic.Int32
}

func (r *receiver) Status(context.Context, *kv.StatusRequest) (*kv.StatusResponse, error) {
	r.calls.Add(1)
	return &kv.StatusResponse{NodeId: "node-1"}, nil
}
func (r *receiver) RequestVote(context.Context, *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	r.calls.Add(1)
	return &raftpb.RequestVoteResponse{VoteGranted: true}, nil
}
func serve(t *testing.T, b *Bundle, peers bool) (string, *receiver) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(b.Options(peers)...)
	r := &receiver{}
	kv.RegisterKVServiceServer(server, r)
	raftpb.RegisterRaftServiceServer(server, r)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String(), r
}
func TestSecureListenerHandshakeAndRoleSeparation(t *testing.T) {
	dir, b := fixture(t)
	foreign, _ := fixture(t)
	clientCreds := func(dir, id string) credentials.TransportCredentials {
		creds, err := ClientCredentials(filepath.Join(dir, "ca.pem"), filepath.Join(dir, id+".pem"), filepath.Join(dir, id+".key"), "")
		if err != nil {
			t.Fatal(err)
		}
		return creds
	}
	for _, test := range []struct {
		name    string
		peers   bool
		creds   credentials.TransportCredentials
		sender  string
		allowed bool
	}{
		{"authorized client", false, clientCreds(dir, "tenant-a"), "", true},
		{"unknown CA", false, clientCreds(foreign, "tenant-a"), "", false},
		{"unregistered signed client", false, clientCreds(dir, "unregistered"), "", false},
		{"node cannot use client listener", false, clientCreds(dir, "node-2"), "", false},
		{"plaintext client rejected", false, insecure.NewCredentials(), "", false},
		{"wrong destination peer", true, b.PeerCredentials("node-2"), "node-1", false},
		{"tenant cannot use peer listener", true, clientCreds(dir, "tenant-a"), "node-2", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			address, r := serve(t, b, test.peers)
			connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(test.creds))
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			if test.peers {
				_, err = raftpb.NewRaftServiceClient(connection).RequestVote(ctx, &raftpb.RequestVoteRequest{CandidateId: test.sender})
			} else {
				_, err = kv.NewKVServiceClient(connection).Status(ctx, &kv.StatusRequest{})
			}
			if test.allowed {
				if err != nil || r.calls.Load() != 1 {
					t.Fatalf("authorized RPC: %v", err)
				}
			} else if err == nil || r.calls.Load() != 0 {
				t.Fatalf("unauthorized request reached handler: %v", err)
			}
		})
	}
	// Use node-2's own identity to check the successful peer path and forged sender.
	node2, err := Load(nodeConfig(dir, "node-2"), "node-2", []string{"node-1"})
	if err != nil {
		t.Fatal(err)
	}
	address, r := serve(t, b, true)
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(node2.PeerCredentials("node-1")))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	service := raftpb.NewRaftServiceClient(connection)
	if _, err := service.RequestVote(ctx, &raftpb.RequestVoteRequest{CandidateId: "node-2"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.RequestVote(ctx, &raftpb.RequestVoteRequest{CandidateId: "node-3"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("forged sender: %v", err)
	}
	if r.calls.Load() != 1 {
		t.Fatal("forged sender mutated handler")
	}
}
func TestInvalidConfigurationFailsClosed(t *testing.T) {
	dir, _ := fixture(t)
	cfg := nodeConfig(dir, "node-1")
	for _, c := range []domain.Config{{CAFile: cfg.CAFile}, {CAFile: cfg.CAFile, CertFile: cfg.CertFile, KeyFile: cfg.KeyFile}, {CAFile: "missing", CertFile: cfg.CertFile, KeyFile: cfg.KeyFile, ClientsFile: cfg.ClientsFile}} {
		if _, err := Load(c, "node-1", nil); err == nil {
			t.Fatal("partial TLS configuration accepted")
		}
	}
	if _, err := Load(cfg, "node-2", nil); err == nil {
		t.Fatal("wrong local identity accepted")
	}
	for _, rules := range [][]domain.Rule{{}, {{Identity: "a", Tenant: "../b", Read: true}}, {{Identity: "a", Tenant: "a", Read: true}, {Identity: "a", Tenant: "b", Read: true}}} {
		data, _ := json.Marshal(rules)
		if err := os.WriteFile(cfg.ClientsFile, data, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(cfg, "node-1", nil); err == nil {
			t.Fatal("invalid policy accepted")
		}
	}
	if b, err := Load(domain.Config{}, "node-1", nil); err != nil || b != nil {
		t.Fatal("legacy isolated development mode failed")
	}
	if err := GenerateDevelopmentPKI(dir); err == nil {
		t.Fatal("existing certificates overwritten")
	}
}

func TestExpiredLocalCredentialRejected(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "expired")
	if err := generateDevelopmentPKI(dir, time.Now().Add(-25*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(nodeConfig(dir, "node-1"), "node-1", nil); err == nil {
		t.Fatal("expired node credentials accepted")
	}
}
