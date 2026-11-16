package application

import (
	"sync"
	"testing"

	"github.com/nickemma/meridian/internal/raft/domain"
)

func TestRunElectionIgnoresStaleTimerAfterHeartbeat(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})
	n.state.RecordHeartbeat()
	n.runElection()

	if got := n.state.CurrentTerm(); got != 0 {
		t.Fatalf("term = %d after recent heartbeat, want 0", got)
	}
	if got := n.state.GetRole(); got != domain.Follower {
		t.Fatalf("role = %s after recent heartbeat, want follower", got)
	}

	// Keep the timer from affecting another test.
	n.electionTimer.Stop()
}

func TestHandleRequestVoteGrantsAtMostOneVotePerTerm(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})
	requests := []domain.VoteRequest{
		{Term: 1, CandidateID: "node-2"},
		{Term: 1, CandidateID: "node-3"},
	}
	responses := make([]domain.VoteResponse, len(requests))
	var wait sync.WaitGroup
	for index, request := range requests {
		wait.Add(1)
		go func(index int, request domain.VoteRequest) {
			defer wait.Done()
			responses[index] = n.handleRequestVote(request)
		}(index, request)
	}
	wait.Wait()

	granted := 0
	for _, response := range responses {
		if response.VoteGranted {
			granted++
		}
	}
	if granted != 1 {
		t.Fatalf("granted %d votes in one term, want exactly 1", granted)
	}
}

func TestLeaderRejectsPreVoteBeforeFirstHeartbeat(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})
	n.state.BecomeCandidate()
	n.state.BecomeLeader([]string{"node-2", "node-3"})

	response := n.handlePreVote(domain.PreVoteRequest{NextTerm: 2, CandidateID: "node-2"})
	if response.VoteGranted {
		t.Fatal("leader granted a pre-vote before sending a heartbeat")
	}
}
