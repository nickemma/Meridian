package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/nickemma/meridian/internal/causal"
	"github.com/nickemma/meridian/internal/config"
	"github.com/nickemma/meridian/internal/consistency"
	"github.com/nickemma/meridian/internal/eventual"
	"github.com/nickemma/meridian/internal/raft"
	"github.com/nickemma/meridian/internal/replicationstore"
	pb "github.com/nickemma/meridian/proto/raft"
	"google.golang.org/grpc"
)

// Server is the gRPC server that handles all incoming RPCs.
type Server struct {
	cfg        *config.Config
	grpcServer *grpc.Server
	clientGRPC *grpc.Server
	raftNode   *raft.Node
	store      raft.KVStore
	causal     *causal.Replica
	eventual   *eventual.Replica
	policies   *consistency.PolicyRegistry
	closeStore func() error
}

// New creates a server with Raft state recovered from durable local storage.
func New(cfg *config.Config) (*Server, error) {
	policies, err := policyRegistry(cfg.Policies)
	if err != nil {
		return nil, err
	}
	machine, store, closeStore, err := newStateMachine(cfg)
	if err != nil {
		return nil, err
	}
	causalStore, err := replicationstore.New(store, "causal")
	if err != nil {
		_ = closeStore()
		return nil, fmt.Errorf("create causal snapshot store: %w", err)
	}
	causalReplica, err := causal.OpenReplica(cfg.NodeID, causalStore)
	if err != nil {
		_ = closeStore()
		return nil, fmt.Errorf("open causal replica: %w", err)
	}
	eventualStore, err := replicationstore.New(store, "eventual")
	if err != nil {
		_ = closeStore()
		return nil, fmt.Errorf("create eventual snapshot store: %w", err)
	}
	eventualReplica, err := eventual.OpenReplica(cfg.NodeID, eventualStore)
	if err != nil {
		_ = closeStore()
		return nil, fmt.Errorf("open eventual replica: %w", err)
	}
	node, err := raft.NewDurableNode(cfg, raftWatermarkStateMachine{next: machine, causal: causalReplica, eventual: eventualReplica, policies: policies})
	if err != nil {
		_ = closeStore()
		return nil, fmt.Errorf("create durable raft node: %w", err)
	}
	return &Server{cfg: cfg, raftNode: node, store: store, causal: causalReplica, eventual: eventualReplica, policies: policies, closeStore: closeStore}, nil
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

	s.grpcServer = grpc.NewServer()
	pb.RegisterRaftServiceServer(s.grpcServer, s.raftNode)
	registerCausalReplicationService(s.grpcServer, s.causal)
	registerEventualReplicationService(s.grpcServer, s.eventual)
	s.clientGRPC = grpc.NewServer()
	registerKVService(s.clientGRPC, s.raftNode, s.store, s.causal, s.eventual, s.policies, newCausalPublisher(s.cfg.Peers), newEventualPublisher(s.cfg.Peers))

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
	go s.runEventualGossip(ctx)

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

func (s *Server) runEventualGossip(ctx context.Context) {
	publish := newEventualPublisher(s.cfg.Peers)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			publish(s.eventual.Records())
		}
	}
}

func policyRegistry(configured []consistency.NamespacePolicy) (*consistency.PolicyRegistry, error) {
	registry := consistency.NewPolicyRegistry()
	if len(configured) == 0 {
		configured = []consistency.NamespacePolicy{{Prefix: "/", Class: consistency.Strong, Version: 1}}
	}
	for _, policy := range configured {
		if err := registry.Install(policy); err != nil {
			return nil, fmt.Errorf("install namespace policy %q: %w", policy.Prefix, err)
		}
	}
	return registry, nil
}

func (s *Server) RaftNode() *raft.Node {
	return s.raftNode
}
