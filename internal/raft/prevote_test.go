package raft

import (
	"testing"
	"time"

	pb "github.com/nickemma/meridian/proto/raft"
)

func TestHandlePreVote_GrantsWhenNoRecentLeader(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})
	// lastHeartbeat is zero — no leader heard from

	resp := n.handlePreVote(&pb.PreVoteRequest{
		NextTerm:     1,
		CandidateId:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !resp.VoteGranted {
		t.Error("expected pre-vote granted when no recent leader")
	}
}

func TestHandlePreVote_DeniesWhenLeaderAlive(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})

	// Simulate a recent heartbeat
	n.state.RecordHeartbeat()

	resp := n.handlePreVote(&pb.PreVoteRequest{
		NextTerm:     1,
		CandidateId:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if resp.VoteGranted {
		t.Error("expected pre-vote denied when leader is alive")
	}
}

func TestHandlePreVote_DeniesStaleCandidate(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})
	n.state.BecomeCandidate() // term = 1

	// Candidate proposes next_term=1 but we are already in term 1
	resp := n.handlePreVote(&pb.PreVoteRequest{
		NextTerm:     1,
		CandidateId:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if resp.VoteGranted {
		t.Error("expected pre-vote denied for stale next_term")
	}
}

func TestHandlePreVote_DeniesStaleLog(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})

	// We have entries the candidate does not
	n.state.AppendEntry(1, []byte("cmd"))

	resp := n.handlePreVote(&pb.PreVoteRequest{
		NextTerm:     1,
		CandidateId:  "node-2",
		LastLogIndex: 0, // candidate has no log — stale
		LastLogTerm:  0,
	})

	if resp.VoteGranted {
		t.Error("expected pre-vote denied for stale log")
	}
}

func TestHeardFromLeaderRecently_FalseWhenNeverHeard(t *testing.T) {
	s := NewState("node-1")
	if s.HeardFromLeaderRecently(300 * time.Millisecond) {
		t.Error("expected false when never heard from leader")
	}
}

func TestHeardFromLeaderRecently_TrueAfterRecordHeartbeat(t *testing.T) {
	s := NewState("node-1")
	s.RecordHeartbeat()
	if !s.HeardFromLeaderRecently(300 * time.Millisecond) {
		t.Error("expected true immediately after recording heartbeat")
	}
}

func TestHeardFromLeaderRecently_FalseAfterTimeout(t *testing.T) {
	s := NewState("node-1")
	s.RecordHeartbeat()

	// Wait longer than the window
	time.Sleep(50 * time.Millisecond)

	if s.HeardFromLeaderRecently(10 * time.Millisecond) {
		t.Error("expected false after timeout window elapsed")
	}
}
