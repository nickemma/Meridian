package server

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/nickemma/meridian/internal/config"
	pb "github.com/nickemma/meridian/proto/raft"
	"google.golang.org/grpc"
)

// Server is the gRPC server that handles all incoming RPCs.
// Right now it only serves Raft RPCs — client RPCs come later.
type Server struct {
	cfg        *config.Config
	grpcServer *grpc.Server

	// raftHandler will be wired in Phase 3 when we build the Raft engine.
	// For now the server compiles and starts without it.
	raftHandler pb.RaftServiceServer
}

// New creates a new Server. The raftHandler is nil for now —
// we'll inject it when Raft is built.
func New(cfg *config.Config) *Server {
	return &Server{
		cfg: cfg,
	}
}

// Start begins listening for incoming Raft RPCs.
// This call blocks until the context is cancelled.

func (s *Server) Start(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.cfg.RaftPort)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.grpcServer = grpc.NewServer()

	// Register the Raft service with a stub handler for now.
	// When we build the real Raft node in Phase 3, we swap this out.
	pb.RegisterRaftServiceServer(s.grpcServer, &stubRaftHandler{})

	log.Printf("[server] node %s listening for Raft RPCs on %s",
		s.cfg.NodeID, addr)

	// Run the gRPC server in a goroutine so we can handle
	// context cancellation (graceful shutdown) below.
	errCh := make(chan error, 1)
	go func() {
		if err := s.grpcServer.Serve(listener); err != nil {
			errCh <- err
		}
	}()

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

// stubRaftHandler satisfies the RaftServiceServer interface
// with empty implementations. This lets the server compile and run
// before the real Raft engine exists.
// Every method here will be replaced in Phase 3.
type stubRaftHandler struct {
	pb.UnimplementedRaftServiceServer
}

func (h *stubRaftHandler) RequestVote(
	ctx context.Context,
	req *pb.RequestVoteRequest,
) (*pb.RequestVoteResponse, error) {
	log.Printf("[stub] RequestVote from candidate %s for term %d",
		req.CandidateId, req.Term)
	return &pb.RequestVoteResponse{
		Term:        req.Term,
		VoteGranted: false, // stub always denies — real logic comes in Phase 3
	}, nil
}

func (h *stubRaftHandler) AppendEntries(
	ctx context.Context,
	req *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {
	log.Printf("[stub] AppendEntries from leader %s term %d entries=%d",
		req.LeaderId, req.Term, len(req.Entries))
	return &pb.AppendEntriesResponse{
		Term:       req.Term,
		Success:    false, // stub always rejects — real logic comes in Phase 3
		MatchIndex: 0,
	}, nil
}
