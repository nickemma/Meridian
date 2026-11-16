package application

import (
	"context"
	"log"
	"sync"

	"github.com/nickemma/meridian/internal/raft/domain"
	"github.com/nickemma/meridian/internal/raft/ports"
)

// runPreVote runs the pre-vote phase before starting a real election.
// Returns true if a majority granted the pre-vote — safe to proceed.
// Returns false if the cluster is healthy or we are too stale to win.
//
// Critically: this function never increments the term.
// It asks "would you vote for me in term+1?" without committing to it.
func (n *Node) runPreVote() bool {
	currentTerm := n.state.CurrentTerm()

	log.Printf("[raft] %s starting pre-vote for next term %d",
		n.state.NodeID(), currentTerm+1)

	req := domain.PreVoteRequest{
		NextTerm:     currentTerm + 1, // what we WOULD use
		CandidateID:  n.state.NodeID(),
		LastLogIndex: n.state.LastLogIndex(),
		LastLogTerm:  n.state.LastLogTerm(),
	}

	// We count our own pre-vote.
	var (
		mu          sync.Mutex
		preVotesWon = 1
		responded   = 0
		done        = make(chan struct{}, 1)
	)

	totalPeers := len(n.peers)

	for _, peer := range n.peers {
		go func(peer ports.Peer) {
			resp, err := peer.PreVote(context.Background(), req)

			mu.Lock()
			defer mu.Unlock()

			responded++

			if err != nil {
				log.Printf("[raft] %s no pre-vote response from %s: %v",
					n.state.NodeID(), peer.ID(), err)
			} else if resp.VoteGranted {
				preVotesWon++
				log.Printf("[raft] %s received pre-vote from %s (total: %d)",
					n.state.NodeID(), peer.ID(), preVotesWon)
			} else {
				log.Printf("[raft] %s pre-vote denied by %s (their term: %d)",
					n.state.NodeID(), peer.ID(), resp.Term)
			}

			if preVotesWon >= n.quorumSize || responded == totalPeers {
				select {
				case done <- struct{}{}:
				default:
				}
			}
		}(peer)
	}

	<-done
	mu.Lock()
	won := preVotesWon >= n.quorumSize
	finalPreVotes := preVotesWon
	mu.Unlock()

	if won {
		log.Printf("[raft] %s pre-vote succeeded (%d votes) — proceeding to election",
			n.state.NodeID(), finalPreVotes)
	} else {
		log.Printf("[raft] %s pre-vote failed (%d votes) — cluster is healthy",
			n.state.NodeID(), finalPreVotes)
	}

	return won
}

// handlePreVote processes an incoming PreVote RPC.
// The voter grants a pre-vote if:
//
//	① It has not heard from a leader recently (cluster might be leaderless)
//	② The candidate's log is at least as up-to-date as ours
//	③ The candidate's next_term is > our current term
//
// Note: we do NOT check votedFor here — pre-vote does not consume a vote.
// A node can grant pre-votes to multiple candidates in the same term
// because no term increment has happened yet.
func (n *Node) handlePreVote(
	req domain.PreVoteRequest,
) domain.PreVoteResponse {
	currentTerm := n.state.CurrentTerm()
	if n.state.GetRole() == domain.Leader {
		log.Printf("[raft] %s denying pre-vote to %s — leader is alive", n.state.NodeID(), req.CandidateID)
		return domain.PreVoteResponse{Term: currentTerm, VoteGranted: false}
	}

	// Deny if candidate's proposed term is behind ours.
	if req.NextTerm <= currentTerm {
		log.Printf("[raft] %s denying pre-vote to %s — stale next_term %d < %d",
			n.state.NodeID(), req.CandidateID, req.NextTerm, currentTerm)
		return domain.PreVoteResponse{
			Term:        currentTerm,
			VoteGranted: false,
		}
	}

	// Deny if we have heard from a leader recently.
	// The cluster is healthy — no election is needed.
	if n.state.HeardFromLeaderRecently(n.opts.ElectionTimeoutMax) {
		log.Printf("[raft] %s denying pre-vote to %s — leader is alive",
			n.state.NodeID(), req.CandidateID)
		return domain.PreVoteResponse{
			Term:        currentTerm,
			VoteGranted: false,
		}
	}

	// Check log up-to-date — same rule as RequestVote.
	if !domain.CandidateLogUpToDate(req.LastLogTerm, req.LastLogIndex, n.state.LastLogTerm(), n.state.LastLogIndex()) {
		log.Printf("[raft] %s denying pre-vote to %s — log not up to date",
			n.state.NodeID(), req.CandidateID)
		return domain.PreVoteResponse{
			Term:        currentTerm,
			VoteGranted: false,
		}
	}

	log.Printf("[raft] %s granting pre-vote to %s for next term %d",
		n.state.NodeID(), req.CandidateID, req.NextTerm)

	return domain.PreVoteResponse{
		Term:        currentTerm,
		VoteGranted: true,
	}
}
