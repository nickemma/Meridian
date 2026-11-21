package application

import (
	"context"
	"log"
	"sync"

	"github.com/nickemma/meridian/internal/raft/domain"
	"github.com/nickemma/meridian/internal/raft/ports"
)

// runElection starts a new election for this node.
// Called when the election timer fires.
//
// The election runs entirely in the background — this function
// returns immediately and the result is applied asynchronously.
// If the node wins, it calls BecomeLeader. If it loses or sees
// a higher term, it calls BecomeFollower.
func (n *Node) runElection() {
	// A timer notification can have been queued immediately before a valid
	// heartbeat reset it. Check the actual heartbeat time before starting a
	// network election, so that stale timer notifications cannot depose a
	// newly elected leader.
	if n.state.GetRole() == domain.Leader || n.state.HeardFromLeaderRecently(n.opts.ElectionTimeoutMin) {
		n.electionTimer.Reset()
		return
	}

	// Phase 1 — pre-vote check.
	// Ask peers if we would win a real election before committing
	// to incrementing our term. If the cluster is healthy,
	// pre-vote will be denied and we do nothing.
	if !n.runPreVote() {
		log.Printf("[raft] %s pre-vote failed, skipping election",
			n.state.NodeID())
		n.electionTimer.Reset() // reset timer to try again later
		return
	}

	// Phase 2 — real election.
	// Pre-vote succeeded — safe to increment term and run.
	term := n.state.BecomeCandidate()
	if err := n.state.DurabilityError(); err != nil {
		log.Printf("[raft] %s cannot start election: persist term %d: %v", n.state.NodeID(), term, err)
		n.electionTimer.Reset()
		return
	}

	log.Printf("[raft] %s starting election for term %d",
		n.state.NodeID(), term)

	// Record election start
	if n.metrics != nil {
		n.metrics.ElectionStarted()
	}
	defer n.recordMetrics()

	// Reset the election timer so we don't immediately re-fire
	// while waiting for vote responses.
	n.electionTimer.Reset()

	// Build the RequestVote request using our current log state.
	req := domain.VoteRequest{
		Term:         term,
		CandidateID:  n.state.NodeID(),
		LastLogIndex: n.state.LastLogIndex(),
		LastLogTerm:  n.state.LastLogTerm(),
	}

	// We start with 1 vote — our own.
	var (
		mu        sync.Mutex
		votesWon  = 1
		responded = 0
		// Votes can arrive before the result goroutine starts waiting.
		// Retain the notification so a fast quorum cannot be lost.
		done = make(chan struct{}, 1)
	)

	totalPeers := len(n.peers)

	// Send RequestVote to all peers simultaneously.
	for _, peer := range n.peers {
		go func(peer ports.Peer) {
			resp, err := peer.RequestVote(context.Background(), req)

			mu.Lock()
			defer mu.Unlock()

			responded++

			if err != nil {
				// Peer unreachable — count as no vote, not a failure.
				log.Printf("[raft] %s no response from %s: %v",
					n.state.NodeID(), peer.ID(), err)
			} else if resp.Term > term {
				// Peer has a higher term — we are stale.
				// Step down immediately regardless of vote result.
				log.Printf("[raft] %s saw higher term %d from %s, stepping down",
					n.state.NodeID(), resp.Term, peer.ID())
				n.state.BecomeFollower(resp.Term)
				if err := n.state.DurabilityError(); err != nil {
					log.Printf("[raft] %s failed to persist higher term %d: %v", n.state.NodeID(), resp.Term, err)
				}
				n.electionTimer.Reset()
				// Signal done so the result goroutine exits cleanly.
				select {
				case done <- struct{}{}:
				default:
				}
				return
			} else if resp.VoteGranted {
				votesWon++
				log.Printf("[raft] %s received vote from %s (total: %d)",
					n.state.NodeID(), peer.ID(), votesWon)
			}

			// Check if we have heard from all peers or won already.
			if votesWon >= n.quorumSize || responded == totalPeers {
				select {
				case done <- struct{}{}:
				default:
				}
			}
		}(peer)
	}

	// Wait for quorum or all peers to respond.
	go func() {
		<-done
		mu.Lock()
		won := votesWon >= n.quorumSize
		finalVotes := votesWon
		mu.Unlock()

		// Check we are still a Candidate in the same term.
		// We might have stepped down already if we saw a higher term.
		if n.state.GetRole() != domain.Candidate || n.state.CurrentTerm() != term {
			return
		}

		if won {
			log.Printf("[raft] %s won election for term %d with %d votes",
				n.state.NodeID(), term, finalVotes)

			n.state.BecomeLeader(n.peerIDs())

			// Pause the election timer — leaders use a heartbeat ticker. Stepping
			// down later re-arms it through Reset.
			n.electionTimer.Pause()

			// Start sending heartbeats immediately.
			go n.runHeartbeat()
		} else {
			log.Printf("[raft] %s lost election for term %d with %d votes",
				n.state.NodeID(), term, finalVotes)
			// Stay as Candidate — election timer will fire again
			// and start a new election.
		}
	}()
}

// handleRequestVote processes an incoming RequestVote RPC.
// Called by the gRPC server when a peer asks for our vote.
func (n *Node) handleRequestVote(
	req domain.VoteRequest,
) domain.VoteResponse {
	n.protocolMu.Lock()
	defer n.protocolMu.Unlock()

	currentTerm := n.state.CurrentTerm()

	// Rule 1 — if the candidate's term is behind ours, deny immediately.
	if req.Term < currentTerm {
		log.Printf("[raft] %s denying vote to %s — stale term %d < %d",
			n.state.NodeID(), req.CandidateID, req.Term, currentTerm)
		return domain.VoteResponse{
			Term:        currentTerm,
			VoteGranted: false,
		}
	}

	// Rule 2 — if we see a higher term, update and step down.
	if req.Term > currentTerm {
		n.state.BecomeFollower(req.Term)
		if err := n.state.DurabilityError(); err != nil {
			log.Printf("[raft] %s cannot persist term %d while handling vote: %v", n.state.NodeID(), req.Term, err)
			return domain.VoteResponse{Term: currentTerm, VoteGranted: false}
		}
		currentTerm = req.Term
		n.electionTimer.Reset()
	}

	votedFor := n.state.VotedFor()

	// Rule 3 — check we haven't already voted for someone else this term.
	alreadyVoted := votedFor != "" && votedFor != req.CandidateID
	if alreadyVoted {
		log.Printf("[raft] %s denying vote to %s — already voted for %s in term %d",
			n.state.NodeID(), req.CandidateID, votedFor, currentTerm)
		return domain.VoteResponse{
			Term:        currentTerm,
			VoteGranted: false,
		}
	}

	// Rule 4 — check the candidate's log is at least as up-to-date as ours.
	// This prevents a stale node from becoming leader and overwriting
	// entries that are already committed on a majority.
	if !domain.CandidateLogUpToDate(req.LastLogTerm, req.LastLogIndex, n.state.LastLogTerm(), n.state.LastLogIndex()) {
		log.Printf("[raft] %s denying vote to %s — log not up to date",
			n.state.NodeID(), req.CandidateID)
		return domain.VoteResponse{
			Term:        currentTerm,
			VoteGranted: false,
		}
	}

	// All rules passed — grant the vote.
	n.state.GrantVote(req.CandidateID)
	if err := n.state.DurabilityError(); err != nil {
		log.Printf("[raft] %s cannot persist vote for %s: %v", n.state.NodeID(), req.CandidateID, err)
		return domain.VoteResponse{Term: currentTerm, VoteGranted: false}
	}
	n.electionTimer.Reset() // reset timer — we just heard from a valid candidate

	log.Printf("[raft] %s granting vote to %s for term %d",
		n.state.NodeID(), req.CandidateID, currentTerm)

	return domain.VoteResponse{
		Term:        currentTerm,
		VoteGranted: true,
	}
}
