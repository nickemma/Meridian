package server

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/nickemma/meridian/internal/config"
	"github.com/nickemma/meridian/internal/raft"
	pb "github.com/nickemma/meridian/proto/raft"
	"google.golang.org/grpc"
)

// Server is the gRPC server that handles all incoming RPCs.
type Server struct {
	cfg        *config.Config
	grpcServer *grpc.Server
	raftNode   *raft.Node
}

// New creates a new Server with a real Raft node
func New(cfg *config.Config) *Server {
	return &Server{
		cfg:      cfg,
		raftNode: raft.NewNode(cfg),
	}
}

// Start begins listening for incoming Raft RPCs and runs the Raft node.
func (s *Server) Start(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.cfg.RaftPort)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.grpcServer = grpc.NewServer()

	// Register the Raft service with raft node
	pb.RegisterRaftServiceServer(s.grpcServer, s.raftNode)

	log.Printf("[server] node %s listening for Raft RPCs on %s",
		s.cfg.NodeID, addr)

	log.Printf("[server] node %s listening for Raft RPCs on %s",
		s.cfg.NodeID, addr)

	errCh := make(chan error, 1)
	go func() {
		if err := s.grpcServer.Serve(listener); err != nil {
			errCh <- err
		}
	}()

	// Run the Raft node in a goroutine.
	go s.raftNode.Run(ctx)

	// Block until context is cancelled (e.g. SIGTERM) or server errors.
	select {
	case <-ctx.Done():
		log.Printf("[server] shutting down node %s", s.cfg.NodeID)
		s.grpcServer.GracefulStop()
		return nil
	case err := <-errCh:
		return fmt.Errorf("gRPC server error: %w", err)
	}
}
