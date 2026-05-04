package raft

import (
	"testing"
	"time"

	"github.com/nickemma/meridian/internal/config"
	pb "github.com/nickemma/meridian/proto/raft"
)

// newTestNode builds a minimal Node for unit testing
// without real network connections or timers.
func newTestNode(id string, peers []string) *Node {
	state := NewState(id)
	peerClients := make([]*PeerClient, len(peers))
	for i, p := range peers {
		peerClients[i] = NewPeerClient(p, p+":9090")
	}
	return &Node{
		state:         state,
		electionTimer: NewElectionTimer(0, 0),
		peers:         peerClients,
		quorumSize:    (len(peers)+1)/2 + 1,
		commitCh:      make(chan uint64, 64),
		cfg: &config.Config{
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
		},
	}
}

func TestHandleAppendEntries_RejectsStaleLeader(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})
	n.state.BecomeCandidate() // term = 1

	// Leader claims term 0 — behind us
	resp := n.handleAppendEntries(&pb.AppendEntriesRequest{
		Term:     0,
		LeaderId: "node-2",
	})

	if resp.Success {
		t.Error("expected rejection of stale leader")
	}
}

func TestHandleAppendEntries_AcceptsHeartbeat(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})

	resp := n.handleAppendEntries(&pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "node-2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	})

	if !resp.Success {
		t.Error("expected heartbeat to succeed")
	}
	if n.state.LeaderID() != "node-2" {
		t.Errorf("expected leader to be node-2, got %s", n.state.LeaderID())
	}
}

func TestHandleAppendEntries_AppendsEntries(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})

	resp := n.handleAppendEntries(&pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "node-2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []*pb.LogEntry{
			{Index: 1, Term: 1, Command: []byte("set x=1")},
			{Index: 2, Term: 1, Command: []byte("set y=2")},
		},
		LeaderCommit: 0,
	})

	if !resp.Success {
		t.Error("expected append to succeed")
	}
	if n.state.LastLogIndex() != 2 {
		t.Errorf("expected last index 2, got %d", n.state.LastLogIndex())
	}
}

func TestHandleAppendEntries_RejectsConsistencyFailure(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})

	// Node has entry at index 1, term 1
	n.state.AppendEntry(1, []byte("cmd"))

	// Leader claims prevLogTerm=2 at index 1 — mismatch
	resp := n.handleAppendEntries(&pb.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "node-2",
		PrevLogIndex: 1,
		PrevLogTerm:  2, // we have term 1 at index 1
		Entries: []*pb.LogEntry{
			{Index: 2, Term: 2, Command: []byte("new cmd")},
		},
	})

	if resp.Success {
		t.Error("expected rejection on term mismatch")
	}
	// Log should be truncated from the conflict point
	if n.state.LastLogIndex() != 0 {
		t.Errorf("expected log truncated to 0, got %d", n.state.LastLogIndex())
	}
}

func TestHandleAppendEntries_AdvancesCommitIndex(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})

	// Append 3 entries
	n.handleAppendEntries(&pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "node-2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []*pb.LogEntry{
			{Index: 1, Term: 1, Command: []byte("a")},
			{Index: 2, Term: 1, Command: []byte("b")},
			{Index: 3, Term: 1, Command: []byte("c")},
		},
		LeaderCommit: 2, // leader has committed up to index 2
	})

	if n.state.CommitIndex() != 2 {
		t.Errorf("expected commit index 2, got %d", n.state.CommitIndex())
	}
}

func TestHandleAppendEntries_HigherTermStepsDown(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})
	// Make node-1 think it's a candidate in term 2
	n.state.BecomeCandidate()
	n.state.BecomeCandidate() // term = 2

	// Receive AppendEntries from term 3
	resp := n.handleAppendEntries(&pb.AppendEntriesRequest{
		Term:     3,
		LeaderId: "node-2",
	})

	if !resp.Success {
		t.Error("expected success after stepping down")
	}
	if n.state.GetRole() != Follower {
		t.Errorf("expected Follower after seeing higher term, got %s",
			n.state.GetRole().string())
	}
	if n.state.CurrentTerm() != 3 {
		t.Errorf("expected term 3, got %d", n.state.CurrentTerm())
	}
}
