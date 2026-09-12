package raft

import (
	"context"
	"log"
	"sync"

	pb "github.com/nickemma/meridian/proto/raft"
)

// handleAppendEntries processes an incoming AppendEntries RPC.
// This handles both heartbeats (empty entries) and real log replication.
func (n *Node) handleAppendEntries(
	req *pb.AppendEntriesRequest,
) *pb.AppendEntriesResponse {
	n.protocolMu.Lock()
	defer n.protocolMu.Unlock()

	currentTerm := n.state.CurrentTerm()

	// Rule 1 — reject if leader's term is behind ours.
	if req.Term < currentTerm {
		return &pb.AppendEntriesResponse{
			Term:    currentTerm,
			Success: false,
		}
	}

	// Rule 2 — a candidate that receives a current-term leader heartbeat must
	// step down. A higher term additionally resets its recorded vote.
	if req.Term > currentTerm {
		n.state.BecomeFollower(req.Term)
		if err := n.state.DurabilityError(); err != nil {
			log.Printf("[raft] %s cannot persist term %d: %v", n.state.NodeID(), req.Term, err)
			return &pb.AppendEntriesResponse{Term: currentTerm, Success: false}
		}
	} else if req.Term == currentTerm && n.state.GetRole() == Candidate {
		n.state.BecomeFollower(req.Term)
	}

	// Valid message from current leader — record it and reset timer.
	n.state.SetLeader(req.LeaderId)
	n.state.RecordHeartbeat()
	n.electionTimer.Reset()

	// Rule 3 — consistency check.
	// If prevLogIndex > 0, we must have a matching entry at that index.
	if req.PrevLogIndex > 0 {
		entry, exists := n.state.EntryAt(req.PrevLogIndex)
		if !exists {
			// We don't have an entry at prevLogIndex — our log is behind.
			log.Printf("[raft] %s rejecting AppendEntries — missing entry at index %d",
				n.state.NodeID(), req.PrevLogIndex)
			return &pb.AppendEntriesResponse{
				Term:    n.state.CurrentTerm(),
				Success: false,
			}
		}
		if entry.Term != req.PrevLogTerm {
			// We have an entry at prevLogIndex but its term doesn't match.
			// Reject it and let the leader retry with an earlier prefix. A
			// follower must never discard a suffix merely because an RPC was
			// rejected: the conflicting entry could already be committed.
			log.Printf("[raft] %s rejecting AppendEntries — term mismatch at index %d"+
				" (have %d, leader has %d)",
				n.state.NodeID(), req.PrevLogIndex, entry.Term, req.PrevLogTerm)
			return &pb.AppendEntriesResponse{
				Term:    n.state.CurrentTerm(),
				Success: false,
			}
		}
	}

	// Rule 4 — append new entries if any.
	if len(req.Entries) > 0 {
		// Convert protobuf entries to our internal LogEntry type.
		entries := make([]LogEntry, len(req.Entries))
		for i, e := range req.Entries {
			entries[i] = LogEntry{
				Index:   e.Index,
				Term:    e.Term,
				Command: e.Command,
			}
		}

		if err := n.state.MergeEntriesFromLeader(req.PrevLogIndex, entries); err != nil {
			log.Printf("[raft] %s cannot merge replicated entries: %v", n.state.NodeID(), err)
			return &pb.AppendEntriesResponse{Term: n.state.CurrentTerm(), Success: false}
		}

	}

	// Rule 5 — advance commit index if leader's is ahead of ours.
	if req.LeaderCommit > n.state.CommitIndex() {
		// Commit index advances to the minimum of leaderCommit
		// and our last log index — we can only commit what we have.
		newCommit := req.LeaderCommit
		if n.state.LastLogIndex() < newCommit {
			newCommit = n.state.LastLogIndex()
		}
		n.state.SetCommitIndex(newCommit)
		if err := n.state.DurabilityError(); err != nil {
			log.Printf("[raft] %s cannot persist commit index %d: %v", n.state.NodeID(), newCommit, err)
			return &pb.AppendEntriesResponse{Term: n.state.CurrentTerm(), Success: false}
		}
		n.notifyApply(newCommit)

		log.Printf("[raft] %s advanced commit index to %d",
			n.state.NodeID(), newCommit)
	}

	return &pb.AppendEntriesResponse{
		Term:    n.state.CurrentTerm(),
		Success: true,
		// This response confirms only the prefix in this request. Returning
		// LastLogIndex would wrongly tell a new leader that an unrelated
		// uncommitted follower suffix also matched its log.
		MatchIndex: req.PrevLogIndex + uint64(len(req.Entries)),
	}
}

// replicateEntries sends the latest log entries to all followers.
// Called by the leader after appending a new client command.
// Also called on each heartbeat tick to catch up lagging followers.
func (n *Node) replicateEntries(ctx context.Context) {
	n.replicationMu.Lock()
	defer n.replicationMu.Unlock()

	var wg sync.WaitGroup

	for _, peer := range n.peers {
		wg.Add(1)
		go func(peer *PeerClient) {
			defer wg.Done()
			n.replicateToPeer(ctx, peer)
		}(peer)
	}

	wg.Wait()
}

// replicateToPeer sends the appropriate log entries to a single peer.
// Uses nextIndex to determine what to send.
// If the peer rejects (consistency check fails), decrements nextIndex and retries.
func (n *Node) replicateToPeer(ctx context.Context, peer *PeerClient) {
	for {
		if n.state.GetRole() != Leader {
			return
		}

		nextIndex := n.state.GetNextIndex(peer.ID())
		prevLogIndex := nextIndex - 1
		prevLogTerm := n.state.TermAt(prevLogIndex)

		// Gather entries to send starting from nextIndex.
		entries := n.state.EntriesFrom(nextIndex)

		// Convert to protobuf entries.
		pbEntries := make([]*pb.LogEntry, len(entries))
		for i, e := range entries {
			pbEntries[i] = &pb.LogEntry{
				Index:   e.Index,
				Term:    e.Term,
				Command: e.Command,
			}
		}

		req := &pb.AppendEntriesRequest{
			Term:         n.state.CurrentTerm(),
			LeaderId:     n.state.NodeID(),
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      pbEntries,
			LeaderCommit: n.state.CommitIndex(),
		}

		resp, err := peer.AppendEntries(ctx, req)
		if err != nil {
			log.Printf("[raft] %s replication to %s failed: %v",
				n.state.NodeID(), peer.ID(), err)
			return
		}

		// Peer has higher term — we are a stale leader, step down.
		if resp.Term > n.state.CurrentTerm() {
			n.state.BecomeFollower(resp.Term)
			if err := n.state.DurabilityError(); err != nil {
				log.Printf("[raft] %s cannot persist higher term %d: %v", n.state.NodeID(), resp.Term, err)
			}
			n.electionTimer.Reset()
			return
		}

		if resp.Success {
			// Peer accepted — update our tracking for this peer.
			n.state.SetMatchIndex(peer.ID(), resp.MatchIndex)
			n.state.SetNextIndex(peer.ID(), resp.MatchIndex+1)
			if len(entries) > 0 {
				log.Printf("[raft] %s replicated to %s up to index %d",
					n.state.NodeID(), peer.ID(), resp.MatchIndex)
			}

			// Recalculate commit index now that this peer has confirmed.
			n.advanceCommitIndex()
			return
		}

		// Peer rejected — consistency check failed.
		// Decrement nextIndex and retry with an earlier entry.
		if nextIndex > 1 {
			n.state.SetNextIndex(peer.ID(), nextIndex-1)
			log.Printf("[raft] %s backing off nextIndex for %s to %d",
				n.state.NodeID(), peer.ID(), nextIndex-1)
		} else {
			// Cannot go below 1 — something is seriously wrong.
			return
		}
	}
}
