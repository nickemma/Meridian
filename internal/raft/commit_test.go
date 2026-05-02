package raft

import (
	"testing"
	"time"
)

func newLeaderNode(id string, peers []string) *Node {
	n := newTestNode(id, peers)
	n.commitCh = make(chan uint64, 64)
	n.state.BecomeCandidate() // term = 1
	n.state.BecomeCandidate() // term = 2 — so term 1 entries are genuinely previous term
	peerIDs := make([]string, len(peers))
	copy(peerIDs, peers)
	n.state.BecomeLeader(peerIDs)
	return n
}

func TestAdvanceCommitIndex_CommitsWhenMajorityConfirms(t *testing.T) {
	n := newLeaderNode("node-1", []string{"node-2", "node-3"})

	// Leader appends an entry in current term
	n.state.AppendEntry(n.state.CurrentTerm(), []byte("cmd"))

	// node-2 confirms index 1
	n.state.SetMatchIndex("node-2", 1)

	// node-1 (leader) has index 1, node-2 has index 1 = majority of 3
	n.advanceCommitIndex()

	if n.state.CommitIndex() != 1 {
		t.Errorf("expected commit index 1, got %d", n.state.CommitIndex())
	}
}

func TestAdvanceCommitIndex_DoesNotCommitWithoutMajority(t *testing.T) {
	n := newLeaderNode("node-1", []string{"node-2", "node-3"})

	n.state.AppendEntry(n.state.CurrentTerm(), []byte("cmd"))

	// No peer has confirmed — only leader has the entry
	n.advanceCommitIndex()

	if n.state.CommitIndex() != 0 {
		t.Errorf("expected commit index 0, got %d", n.state.CommitIndex())
	}
}

func TestAdvanceCommitIndex_DoesNotCommitPreviousTermEntries(t *testing.T) {
	n := newLeaderNode("node-1", []string{"node-2", "node-3"})

	// Append entry from an earlier term (simulating a recovered entry)
	n.state.AppendEntries([]LogEntry{
		{Index: 1, Term: 1, Command: []byte("old")},
	})

	// Both peers confirm it
	n.state.SetMatchIndex("node-2", 1)
	n.state.SetMatchIndex("node-3", 1)

	// Current term is 2 (BecomeCandidate incremented it)
	// Entry at index 1 is from term 1 — must not commit directly
	n.advanceCommitIndex()

	if n.state.CommitIndex() != 0 {
		t.Errorf("expected no commit of previous-term entry, got %d",
			n.state.CommitIndex())
	}
}

func TestAdvanceCommitIndex_CommitNotifiesApplyLoop(t *testing.T) {
	n := newLeaderNode("node-1", []string{"node-2", "node-3"})

	n.state.AppendEntry(n.state.CurrentTerm(), []byte("cmd"))
	n.state.SetMatchIndex("node-2", 1)

	n.advanceCommitIndex()

	// commitCh should have received a signal
	select {
	case idx := <-n.commitCh:
		if idx != 1 {
			t.Errorf("expected commit signal for index 1, got %d", idx)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("expected commit signal on commitCh, got none")
	}
}

func TestApplyLoop_AppliesCommittedEntries(t *testing.T) {
	n := newLeaderNode("node-1", []string{"node-2", "node-3"})

	// Append 3 entries and commit all of them
	for i := 0; i < 3; i++ {
		n.state.AppendEntry(n.state.CurrentTerm(), []byte("cmd"))
	}
	n.state.SetCommitIndex(3)

	// Run applyCommitted directly — simulates what the apply loop does
	n.applyCommitted()

	if n.state.LastApplied() != 3 {
		t.Errorf("expected lastApplied 3, got %d", n.state.LastApplied())
	}
}

func TestApplyLoop_NeverAppliesBeyondCommitIndex(t *testing.T) {
	n := newLeaderNode("node-1", []string{"node-2", "node-3"})

	// Append 5 entries but only commit 3
	for i := 0; i < 5; i++ {
		n.state.AppendEntry(n.state.CurrentTerm(), []byte("cmd"))
	}
	n.state.SetCommitIndex(3)

	n.applyCommitted()

	// Must stop at 3 — entries 4 and 5 are not committed
	if n.state.LastApplied() != 3 {
		t.Errorf("expected lastApplied 3, got %d", n.state.LastApplied())
	}
}
