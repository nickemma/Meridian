package adapters

import (
	"context"

	"github.com/nickemma/meridian/internal/raft/application"
	"github.com/nickemma/meridian/internal/raft/domain"
	pb "github.com/nickemma/meridian/proto/raft"
	"google.golang.org/grpc"
)

// Service exposes a Node's inbound RPCs over gRPC.
type Service struct {
	pb.UnimplementedRaftServiceServer
	node *application.Node
}

// RegisterService registers node's peer RPCs on server.
func RegisterService(server grpc.ServiceRegistrar, node *application.Node) {
	pb.RegisterRaftServiceServer(server, &Service{node: node})
}

func (s *Service) RequestVote(_ context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	resp := s.node.RequestVote(domain.VoteRequest{
		Term: req.GetTerm(), CandidateID: req.GetCandidateId(),
		LastLogIndex: req.GetLastLogIndex(), LastLogTerm: req.GetLastLogTerm(),
	})
	return &pb.RequestVoteResponse{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

func (s *Service) PreVote(_ context.Context, req *pb.PreVoteRequest) (*pb.PreVoteResponse, error) {
	resp := s.node.PreVote(domain.PreVoteRequest{
		NextTerm: req.GetNextTerm(), CandidateID: req.GetCandidateId(),
		LastLogIndex: req.GetLastLogIndex(), LastLogTerm: req.GetLastLogTerm(),
	})
	return &pb.PreVoteResponse{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

func (s *Service) AppendEntries(_ context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	entries := make([]domain.LogEntry, len(req.GetEntries()))
	for i, e := range req.GetEntries() {
		entries[i] = domain.LogEntry{Index: e.GetIndex(), Term: e.GetTerm(), Command: e.GetCommand()}
	}
	resp := s.node.AppendEntries(domain.AppendEntriesRequest{
		Term: req.GetTerm(), LeaderID: req.GetLeaderId(),
		PrevLogIndex: req.GetPrevLogIndex(), PrevLogTerm: req.GetPrevLogTerm(),
		Entries: entries, LeaderCommit: req.GetLeaderCommit(),
	})
	return &pb.AppendEntriesResponse{Term: resp.Term, Success: resp.Success, MatchIndex: resp.MatchIndex}, nil
}

func voteRequestToProto(req domain.VoteRequest) *pb.RequestVoteRequest {
	return &pb.RequestVoteRequest{Term: req.Term, CandidateId: req.CandidateID, LastLogIndex: req.LastLogIndex, LastLogTerm: req.LastLogTerm}
}

func preVoteRequestToProto(req domain.PreVoteRequest) *pb.PreVoteRequest {
	return &pb.PreVoteRequest{NextTerm: req.NextTerm, CandidateId: req.CandidateID, LastLogIndex: req.LastLogIndex, LastLogTerm: req.LastLogTerm}
}

func appendEntriesRequestToProto(req domain.AppendEntriesRequest) *pb.AppendEntriesRequest {
	entries := make([]*pb.LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = &pb.LogEntry{Index: e.Index, Term: e.Term, Command: e.Command}
	}
	return &pb.AppendEntriesRequest{
		Term: req.Term, LeaderId: req.LeaderID, PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm: req.PrevLogTerm, Entries: entries, LeaderCommit: req.LeaderCommit,
	}
}
