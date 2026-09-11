package raft

import (
	"sync"
	"testing"

	pb "github.com/nickemma/meridian/proto/raft"
)

func TestRunElectionIgnoresStaleTimerAfterHeartbeat(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})
	n.state.RecordHeartbeat()
	n.runElection()

	if got := n.state.CurrentTerm(); got != 0 {
		t.Fatalf("term = %d after recent heartbeat, want 0", got)
	}
	if got := n.state.GetRole(); got != Follower {
		t.Fatalf("role = %s after recent heartbeat, want follower", got)
	}

	// Keep the timer from affecting another test.
	n.electionTimer.Stop()
}

func TestHandleRequestVoteGrantsAtMostOneVotePerTerm(t *testing.T) {
	n := newTestNode("node-1", []string{"node-2", "node-3"})
	requests := []*pb.RequestVoteRequest{
		{Term: 1, CandidateId: "node-2"},
		{Term: 1, CandidateId: "node-3"},
	}
	responses := make([]*pb.RequestVoteResponse, len(requests))
	var wait sync.WaitGroup
	for index, request := range requests {
		wait.Add(1)
		go func(index int, request *pb.RequestVoteRequest) {
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

	response := n.handlePreVote(&pb.PreVoteRequest{NextTerm: 2, CandidateId: "node-2"})
	if response.VoteGranted {
		t.Fatal("leader granted a pre-vote before sending a heartbeat")
	}
}
