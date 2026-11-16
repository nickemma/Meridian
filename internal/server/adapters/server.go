// Package adapters is the node's composition root: it wires configuration,
// security, storage, the Raft node and both weak-path replicas behind two
// gRPC listeners (peer and client).
package adapters

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	causaladapters "github.com/nickemma/meridian/internal/causal/adapters"
	causal "github.com/nickemma/meridian/internal/causal/application"
	config "github.com/nickemma/meridian/internal/config/domain"
	codec "github.com/nickemma/meridian/internal/consistency/adapters"
	consistency "github.com/nickemma/meridian/internal/consistency/application"
	consistencyports "github.com/nickemma/meridian/internal/consistency/ports"
	eventualadapters "github.com/nickemma/meridian/internal/eventual/adapters"
	eventual "github.com/nickemma/meridian/internal/eventual/application"
	observability "github.com/nickemma/meridian/internal/observability/adapters"
	raftadapters "github.com/nickemma/meridian/internal/raft/adapters"
	raft "github.com/nickemma/meridian/internal/raft/application"
	raftdomain "github.com/nickemma/meridian/internal/raft/domain"
	replicationstore "github.com/nickemma/meridian/internal/replicationstore/application"
	security "github.com/nickemma/meridian/internal/security/adapters"
	"github.com/nickemma/meridian/internal/server/application"
	"github.com/nickemma/meridian/internal/server/ports"
	storageports "github.com/nickemma/meridian/internal/storage/ports"
	"google.golang.org/grpc"
)

// antiEntropyInterval is how often a node gossips its full eventual state.
const antiEntropyInterval = 250 * time.Millisecond

// Server owns one node's listeners and storage.
type Server struct {
	security   *security.Bundle
	observer   *observability.Observer
	cfg        *config.Config
	grpcServer *grpc.Server
	clientGRPC *grpc.Server
	raftNode   *raft.Node
	store      storageports.KVStore
	causal     *causal.Replica
	eventual   *eventual.Replica
	policies   *consistency.PolicyRegistry
	closeStore func() error
}

// New creates a server with Raft state recovered from durable local storage.
func New(cfg *config.Config) (*Server, error) {
	transport, err := security.Load(cfg.Security, cfg.NodeID, cfg.PeerIDs())
	if err != nil {
		return nil, err
	}
	policies, err := consistency.NewConfiguredRegistry(cfg.Policies)
	if err != nil {
		return nil, err
	}
	machine, store, closeStore, err := newStateMachine(cfg)
	if err != nil {
		return nil, err
	}
	fail := func(format string, err error) (*Server, error) {
		_ = closeStore()
		return nil, fmt.Errorf(format, err)
	}
	causalReplica, err := openCausal(cfg.NodeID, store)
	if err != nil {
		return fail("open causal replica: %w", err)
	}
	eventualReplica, err := openEventual(cfg.NodeID, store)
	if err != nil {
		return fail("open eventual replica: %w", err)
	}
	watermark := application.NewRaftWatermark(machine, causalReplica, eventualReplica, policies)
	node, err := raftadapters.OpenDurableNode(raftOptions(cfg), cfg.DataDir, raftPeers(cfg), transport.PeerCredentials, watermark)
	if err != nil {
		return fail("create durable raft node: %w", err)
	}
	return &Server{security: transport, cfg: cfg, raftNode: node, store: store, causal: causalReplica, eventual: eventualReplica, policies: policies, closeStore: closeStore}, nil
}

func openCausal(nodeID string, store storageports.KVStore) (*causal.Replica, error) {
	snapshots, err := replicationstore.New(store, "causal")
	if err != nil {
		return nil, err
	}
	return causal.OpenReplica(nodeID, snapshots)
}

func openEventual(nodeID string, store storageports.KVStore) (*eventual.Replica, error) {
	snapshots, err := replicationstore.New(store, "eventual")
	if err != nil {
		return nil, err
	}
	return eventual.OpenReplica(nodeID, snapshots)
}

func raftOptions(cfg *config.Config) raftdomain.Options {
	return raftdomain.Options{
		NodeID:             cfg.NodeID,
		ElectionTimeoutMin: cfg.ElectionTimeoutMin,
		ElectionTimeoutMax: cfg.ElectionTimeoutMax,
		HeartbeatInterval:  cfg.HeartbeatInterval,
		QuorumSize:         cfg.QuorumSize,
	}
}

func raftPeers(cfg *config.Config) []raftadapters.PeerAddress {
	peers := make([]raftadapters.PeerAddress, len(cfg.Peers))
	for i, p := range cfg.Peers {
		peers[i] = raftadapters.PeerAddress{ID: p.ID, Address: p.Address}
	}
	return peers
}

func replicationPeers(cfg *config.Config) []codec.Peer {
	peers := make([]codec.Peer, len(cfg.Peers))
	for i, p := range cfg.Peers {
		peers[i] = codec.Peer{ID: p.ID, Address: p.Address}
	}
	return peers
}

// replicationObserver avoids handing a typed-nil observer to the fan-out.
func (s *Server) replicationObserver() consistencyports.ReplicationObserver {
	if s.observer == nil {
		return nil
	}
	return s.observer
}

func (s *Server) dependencyObserver() ports.DependencyObserver {
	if s.observer == nil {
		return nil
	}
	return statusObserver{next: s.observer}
}

// Start listens for peer Raft RPCs and client KV RPCs on separate ports, then
// runs the node until its context ends.
func (s *Server) Start(ctx context.Context) error {
	raftAddr := fmt.Sprintf(":%d", s.cfg.RaftPort)
	raftListener, err := net.Listen("tcp", raftAddr)
	if err != nil {
		return fmt.Errorf("listen for Raft RPCs on %s: %w", raftAddr, err)
	}
	clientAddr := fmt.Sprintf(":%d", s.cfg.ClientPort)
	clientListener, err := net.Listen("tcp", clientAddr)
	if err != nil {
		_ = raftListener.Close()
		return fmt.Errorf("listen for client RPCs on %s: %w", clientAddr, err)
	}

	mode := "plaintext development"
	if s.security != nil {
		mode = "mutual TLS and authorization"
	}
	log.Printf("[server] transport=%s", mode)

	peers := replicationPeers(s.cfg)
	gossip := eventualadapters.NewGossip(peers, s.security.PeerCredentials, s.replicationObserver())

	s.grpcServer = grpc.NewServer(s.security.Options(true)...)
	raftadapters.RegisterService(s.grpcServer, s.raftNode)
	causaladapters.RegisterService(s.grpcServer, s.causal)
	eventualadapters.RegisterService(s.grpcServer, s.eventual)

	clientOptions := []grpc.ServerOption{grpc.ChainUnaryInterceptor(s.observer.Intercept)}
	clientOptions = append(clientOptions, s.security.Options(false)...)
	s.clientGRPC = grpc.NewServer(clientOptions...)
	registerKVService(s.clientGRPC, application.NewKVService(application.Dependencies{
		Consensus: s.raftNode,
		State:     s.store,
		Causal:    s.causal,
		Eventual:  s.eventual,
		Policies:  s.policies,
		Publish:   ports.Publish(causaladapters.NewPublisher(peers, s.security.PeerCredentials, s.replicationObserver())),
		Gossip:    ports.Gossip(gossip),
		Observer:  s.dependencyObserver(),
	}))

	log.Printf("[server] node %s listening for Raft RPCs on %s", s.cfg.NodeID, raftAddr)
	log.Printf("[server] node %s listening for client RPCs on %s", s.cfg.NodeID, clientAddr)

	errCh := make(chan error, 2)
	go func() {
		if err := s.grpcServer.Serve(raftListener); err != nil {
			errCh <- err
		}
	}()
	go func() {
		if err := s.clientGRPC.Serve(clientListener); err != nil {
			errCh <- err
		}
	}()

	// Run the Raft node in a goroutine. Shutdown waits for it after stopping
	// both RPC servers, so no handler or apply loop can touch the state machine
	// after closeStore releases the Rust engine.
	raftDone := make(chan struct{})
	go func() {
		defer close(raftDone)
		s.raftNode.Run(ctx)
	}()
	go s.eventual.RunAntiEntropy(ctx, antiEntropyInterval, gossip)

	// Block until context is cancelled (e.g. SIGTERM) or server errors.
	select {
	case <-ctx.Done():
		log.Printf("[server] shutting down node %s", s.cfg.NodeID)
		s.grpcServer.GracefulStop()
		s.clientGRPC.GracefulStop()
		<-raftDone
		if err := s.closeStore(); err != nil {
			return fmt.Errorf("close state-machine storage: %w", err)
		}
		return nil
	case err := <-errCh:
		return fmt.Errorf("gRPC server error: %w", err)
	}
}

func (s *Server) RaftNode() *raft.Node { return s.raftNode }

// SetObserver is called before Start; gauges read live replica state.
func (s *Server) SetObserver(o *observability.Observer) { s.observer = o }

// PendingDependencies is the causal backlog gauge source.
func (s *Server) PendingDependencies() float64 { return float64(s.causal.Pending()) }

// ApplyGap is local commit index minus local applied index.
func (s *Server) ApplyGap() float64 {
	state := s.raftNode.Status()
	if state.CommitIndex < state.LastApplied {
		return 0
	}
	return float64(state.CommitIndex - state.LastApplied)
}
