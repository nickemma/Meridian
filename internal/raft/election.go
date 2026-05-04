package raft

import (
	"context"
	"log"
	"sync"

	pb "github.com/nickemma/meridian/proto/raft"
)

// runElection starts a new election for this node.
// Called when the election timer fires.
//
// The election runs entirely in the background — this function
// returns immediately and the result is applied asynchronously.
// If the node wins, it calls BecomeLeader. If it loses or sees
// a higher term, it calls BecomeFollower.
func (n *Node) runElection() {

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

	log.Printf("[raft] %s starting election for term %d",
		n.state.NodeID(), term)

	// Reset the election timer so we don't immediately re-fire
	// while waiting for vote responses.
	n.electionTimer.Reset()

	// Build the RequestVote request using our current log state.
	req := &pb.RequestVoteRequest{
		Term:         term,
		CandidateId:  n.state.NodeID(),
		LastLogIndex: n.state.LastLogIndex(),
		LastLogTerm:  n.state.LastLogTerm(),
	}

	// We start with 1 vote — our own.
	var (
		mu        sync.Mutex
		votesWon  = 1
		responded = 0
		done      = make(chan struct{})
	)

	totalPeers := len(n.peers)

	// Send RequestVote to all peers simultaneously.
	for _, peer := range n.peers {
		go func(peer *PeerClient) {
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

		// Check we are still a Candidate in the same term.
		// We might have stepped down already if we saw a higher term.
		if n.state.GetRole() != Candidate || n.state.CurrentTerm() != term {
			return
		}

		if votesWon >= n.quorumSize {
			log.Printf("[raft] %s won election for term %d with %d votes",
				n.state.NodeID(), term, votesWon)

			peerIDs := make([]string, len(n.peers))
			for i, p := range n.peers {
				peerIDs[i] = p.ID()
			}
			n.state.BecomeLeader(peerIDs)

			// Stop the election timer — leaders use a heartbeat ticker.
			n.electionTimer.Stop()

			// Start sending heartbeats immediately.
			go n.runHeartbeat()
		} else {
			log.Printf("[raft] %s lost election for term %d with %d votes",
				n.state.NodeID(), term, votesWon)
			// Stay as Candidate — election timer will fire again
			// and start a new election.
		}
	}()
}

// handleRequestVote processes an incoming RequestVote RPC.
// Called by the gRPC server when a peer asks for our vote.
func (n *Node) handleRequestVote(
	req *pb.RequestVoteRequest,
) *pb.RequestVoteResponse {
	currentTerm := n.state.CurrentTerm()

	// Rule 1 — if the candidate's term is behind ours, deny immediately.
	if req.Term < currentTerm {
		log.Printf("[raft] %s denying vote to %s — stale term %d < %d",
			n.state.NodeID(), req.CandidateId, req.Term, currentTerm)
		return &pb.RequestVoteResponse{
			Term:        currentTerm,
			VoteGranted: false,
		}
	}

	// Rule 2 — if we see a higher term, update and step down.
	if req.Term > currentTerm {
		n.state.BecomeFollower(req.Term)
		currentTerm = req.Term
		n.electionTimer.Reset()
	}

	votedFor := n.state.VotedFor()

	// Rule 3 — check we haven't already voted for someone else this term.
	alreadyVoted := votedFor != "" && votedFor != req.CandidateId
	if alreadyVoted {
		log.Printf("[raft] %s denying vote to %s — already voted for %s in term %d",
			n.state.NodeID(), req.CandidateId, votedFor, currentTerm)
		return &pb.RequestVoteResponse{
			Term:        currentTerm,
			VoteGranted: false,
		}
	}

	// Rule 4 — check the candidate's log is at least as up-to-date as ours.
	// This prevents a stale node from becoming leader and overwriting
	// entries that are already committed on a majority.
	ourLastTerm := n.state.LastLogTerm()
	ourLastIndex := n.state.LastLogIndex()

	candidateLogOk := req.LastLogTerm > ourLastTerm ||
		(req.LastLogTerm == ourLastTerm && req.LastLogIndex >= ourLastIndex)

	if !candidateLogOk {
		log.Printf("[raft] %s denying vote to %s — log not up to date",
			n.state.NodeID(), req.CandidateId)
		return &pb.RequestVoteResponse{
			Term:        currentTerm,
			VoteGranted: false,
		}
	}

	// All rules passed — grant the vote.
	n.state.GrantVote(req.CandidateId)
	n.electionTimer.Reset() // reset timer — we just heard from a valid candidate

	log.Printf("[raft] %s granting vote to %s for term %d",
		n.state.NodeID(), req.CandidateId, currentTerm)

	return &pb.RequestVoteResponse{
		Term:        currentTerm,
		VoteGranted: true,
	}
}
