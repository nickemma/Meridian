package domain

import "errors"

// The RPC messages below are transport-neutral. The gRPC adapter converts
// them to and from protobuf; the protocol core never sees protobuf types.

type VoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type VoteResponse struct {
	Term        uint64
	VoteGranted bool
}

// PreVoteRequest asks "would you vote for me in NextTerm?" without anyone
// incrementing a term.
type PreVoteRequest struct {
	NextTerm     uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type PreVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesRequest is both a heartbeat (no entries) and log replication.
type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
	// MatchIndex confirms only the prefix carried by the request.
	MatchIndex uint64
}

// Status is a read-only node snapshot for the client status RPC.
type Status struct {
	NodeID      string
	Role        string
	Term        uint64
	LeaderID    string
	CommitIndex uint64
	LastApplied uint64
}

// ErrCommandSuperseded means a submitted command's log entry was replaced by a
// different leader's entry at the same index. The command was definitely not
// committed and may be retried on the current leader.
var ErrCommandSuperseded = errors.New("command was superseded by another leader's entry and was not committed")
