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
		stateMachine:  NoopStateMachine{},
		applyWaiters:  make(map[uint64]chan error),
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

func TestHandleAppendEntries_HeartbeatDoesNotAcknowledgeUnrelatedSuffix(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})
	n.state.AppendEntry(1, []byte("uncommitted suffix"))

	response := n.handleAppendEntries(&pb.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "node-2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	})
	if !response.Success {
		t.Fatal("heartbeat failed")
	}
	if response.MatchIndex != 0 {
		t.Fatalf("heartbeat match index = %d, want 0", response.MatchIndex)
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

func TestHandleAppendEntries_ReplayRetainsCommittedPrefix(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})
	first := &pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "node-2",
		PrevLogIndex: 0,
		Entries: []*pb.LogEntry{
			{Index: 1, Term: 1, Command: []byte("first")},
		},
		LeaderCommit: 1,
	}
	if response := n.handleAppendEntries(first); !response.Success {
		t.Fatal("initial append failed")
	}

	// A delayed replication round may replay entry 1 together with entry 2.
	// The follower must keep its committed entry, then append entry 2; it must
	// not truncate index 1 before rebuilding the suffix.
	replay := &pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "node-2",
		PrevLogIndex: 0,
		Entries: []*pb.LogEntry{
			{Index: 1, Term: 1, Command: []byte("first")},
			{Index: 2, Term: 1, Command: []byte("second")},
		},
		LeaderCommit: 1,
	}
	if response := n.handleAppendEntries(replay); !response.Success {
		t.Fatal("replayed append failed")
	}
	if got := n.state.CommitIndex(); got != 1 {
		t.Fatalf("commit index = %d, want 1", got)
	}
	if got := n.state.LastLogIndex(); got != 2 {
		t.Fatalf("last log index = %d, want 2", got)
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
	// A rejected prefix does not itself authorize a follower to discard an
	// entry. A later matching AppendEntries request may replace only an
	// uncommitted conflicting suffix.
	if n.state.LastLogIndex() != 1 {
		t.Errorf("expected log to remain intact at index 1, got %d", n.state.LastLogIndex())
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
	select {
	case index := <-n.commitCh:
		if index != 2 {
			t.Errorf("apply notification index = %d, want 2", index)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected follower commit to notify the apply loop")
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
