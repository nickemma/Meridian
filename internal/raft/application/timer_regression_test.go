package application

import (
	"context"
	"testing"
	"time"

	"github.com/nickemma/meridian/internal/raft/domain"
	"github.com/nickemma/meridian/internal/raft/ports"
)

// votingPeer grants every pre-vote and vote, so an election always succeeds.
type votingPeer struct{ stubPeer }

func (votingPeer) PreVote(_ context.Context, req domain.PreVoteRequest) (domain.PreVoteResponse, error) {
	return domain.PreVoteResponse{Term: req.NextTerm - 1, VoteGranted: true}, nil
}
func (votingPeer) RequestVote(_ context.Context, req domain.VoteRequest) (domain.VoteResponse, error) {
	return domain.VoteResponse{Term: req.Term, VoteGranted: true}, nil
}

// A leader that steps down must campaign again when it later stops hearing
// from a leader. Before the fix, winning an election stopped the election
// timer permanently, so a deposed leader could never start another election
// and a majority containing it could stay leaderless forever.
func TestDeposedLeaderCanCampaignAgain(t *testing.T) {
	n := newTestNode("node-1", nil)
	n.peers = []ports.Peer{votingPeer{stubPeer{id: "node-2"}}, votingPeer{stubPeer{id: "node-3"}}}
	n.opts.HeartbeatInterval = time.Hour // keep the leader's ticker idle
	n.electionTimer = NewElectionTimer(20*time.Millisecond, 40*time.Millisecond)
	defer n.stopOnce.Do(func() { close(n.stopCh) })
	defer n.electionTimer.Stop()

	n.runElection()
	deadline := time.Now().Add(5 * time.Second)
	for n.state.GetRole() != domain.Leader {
		if time.Now().After(deadline) {
			t.Fatal("node did not win the election")
		}
		time.Sleep(time.Millisecond)
	}

	// A higher-term leader deposes it, then goes silent.
	if resp := n.handleAppendEntries(domain.AppendEntriesRequest{Term: 5, LeaderID: "node-2"}); !resp.Success {
		t.Fatal("deposing heartbeat rejected")
	}
	select {
	case <-n.electionTimer.Fired():
	case <-time.After(2 * time.Second):
		t.Fatal("deposed leader's election timer never fired again")
	}
}
