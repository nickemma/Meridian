package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nickemma/meridian/internal/raft/domain"
	"github.com/nickemma/meridian/internal/raft/ports"
)

// stubPeer is an unreachable peer: protocol unit tests never reach the
// network, and any accidental call fails like a partitioned node.
type stubPeer struct{ id string }

var errUnreachable = errors.New("peer unreachable in unit test")

func (p stubPeer) ID() string { return p.id }
func (stubPeer) RequestVote(context.Context, domain.VoteRequest) (domain.VoteResponse, error) {
	return domain.VoteResponse{}, errUnreachable
}
func (stubPeer) PreVote(context.Context, domain.PreVoteRequest) (domain.PreVoteResponse, error) {
	return domain.PreVoteResponse{}, errUnreachable
}
func (stubPeer) AppendEntries(context.Context, domain.AppendEntriesRequest) (domain.AppendEntriesResponse, error) {
	return domain.AppendEntriesResponse{}, errUnreachable
}
func (stubPeer) Close() {}

// newTestNode builds a minimal Node for unit testing
// without real network connections or timers.
func newTestNode(id string, peers []string) *Node {
	transports := make([]ports.Peer, len(peers))
	for i, p := range peers {
		transports[i] = stubPeer{id: p}
	}
	n := NewNode(domain.Options{
		NodeID:             id,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		QuorumSize:         (len(peers)+1)/2 + 1,
	}, NewState(id), transports, NoopStateMachine{})
	n.electionTimer = NewElectionTimer(0, 0)
	return n
}

func TestHandleAppendEntries_RejectsStaleLeader(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})
	n.state.BecomeCandidate() // term = 1

	// domain.Leader claims term 0 — behind us
	resp := n.handleAppendEntries(domain.AppendEntriesRequest{
		Term:     0,
		LeaderID: "node-2",
	})

	if resp.Success {
		t.Error("expected rejection of stale leader")
	}
}

func TestHandleAppendEntries_AcceptsHeartbeat(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})

	resp := n.handleAppendEntries(domain.AppendEntriesRequest{
		Term:         1,
		LeaderID:     "node-2",
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

	response := n.handleAppendEntries(domain.AppendEntriesRequest{
		Term:         2,
		LeaderID:     "node-2",
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

	resp := n.handleAppendEntries(domain.AppendEntriesRequest{
		Term:         1,
		LeaderID:     "node-2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []domain.LogEntry{
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
	first := domain.AppendEntriesRequest{
		Term:         1,
		LeaderID:     "node-2",
		PrevLogIndex: 0,
		Entries: []domain.LogEntry{
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
	replay := domain.AppendEntriesRequest{
		Term:         1,
		LeaderID:     "node-2",
		PrevLogIndex: 0,
		Entries: []domain.LogEntry{
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

	// domain.Leader claims prevLogTerm=2 at index 1 — mismatch
	resp := n.handleAppendEntries(domain.AppendEntriesRequest{
		Term:         2,
		LeaderID:     "node-2",
		PrevLogIndex: 1,
		PrevLogTerm:  2, // we have term 1 at index 1
		Entries: []domain.LogEntry{
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
	n.handleAppendEntries(domain.AppendEntriesRequest{
		Term:         1,
		LeaderID:     "node-2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []domain.LogEntry{
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
	resp := n.handleAppendEntries(domain.AppendEntriesRequest{
		Term:     3,
		LeaderID: "node-2",
	})

	if !resp.Success {
		t.Error("expected success after stepping down")
	}
	if n.state.GetRole() != domain.Follower {
		t.Errorf("expected domain.Follower after seeing higher term, got %s",
			n.state.GetRole().String())
	}
	if n.state.CurrentTerm() != 3 {
		t.Errorf("expected term 3, got %d", n.state.CurrentTerm())
	}
}

// A heartbeat built before a concurrent leader write can carry a later commit
// index than the prefix it actually confirms. It must not commit a divergent
// follower suffix, even when that suffix extends to leaderCommit.
func TestHeartbeatDoesNotCommitUnconfirmedSuffix(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})
	defer n.electionTimer.Stop()
	n.state.AppendEntry(1, []byte("common"))
	n.state.AppendEntry(2, []byte("uncommitted divergent value"))
	n.state.SetCommitIndex(1)
	response := n.handleAppendEntries(domain.AppendEntriesRequest{Term: 3, LeaderID: "node-2", PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 2})
	if !response.Success {
		t.Fatal("matching prefix was rejected")
	}
	if n.state.CommitIndex() != 1 {
		t.Fatalf("committed unconfirmed suffix: commit=%d", n.state.CommitIndex())
	}
}

// recordingMetrics captures the last observed sample.
type recordingMetrics struct{ last ports.Sample }

func (*recordingMetrics) HeartbeatSent()           {}
func (*recordingMetrics) ElectionStarted()         {}
func (m *recordingMetrics) Observe(s ports.Sample) { m.last = s }

func (m *recordingMetrics) lag(peer string) uint64 {
	for _, lag := range m.last.PeerLag {
		if lag.Peer == peer {
			return lag.Entries
		}
	}
	return ^uint64(0)
}

func TestPeerLagReportsConfirmedReplicationProgress(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2"})
	recorder := &recordingMetrics{}
	n.SetMetrics(recorder)
	n.state.BecomeCandidate()
	n.state.BecomeLeader([]string{"node-2"})
	for i := 0; i < 3; i++ {
		n.state.AppendEntry(1, []byte("value"))
	}
	n.state.SetMatchIndex("node-2", 1)
	n.recordMetrics()
	if got := recorder.lag("node-2"); got != 2 {
		t.Fatalf("lag=%d, want 2", got)
	}
	if recorder.last.Role != domain.Leader || recorder.last.LogEntries != 3 {
		t.Fatalf("sample = %+v", recorder.last)
	}
	n.state.BecomeFollower(2)
	n.recordMetrics()
	if got := recorder.lag("node-2"); got != 0 {
		t.Fatal("follower retained a leader replication-lag sample")
	}
}
