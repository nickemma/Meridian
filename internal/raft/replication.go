package raft

import (
	pb "github.com/nickemma/meridian/proto/raft"
)

// handleAppendEntries processes an incoming AppendEntries RPC.
// Full implementation in Phase 3d.
// For now we handle heartbeats — empty entries from the leader.
func (n *Node) handleAppendEntries(
	req *pb.AppendEntriesRequest,
) *pb.AppendEntriesResponse {
	currentTerm := n.state.CurrentTerm()

	// Reject if leader's term is behind ours.
	if req.Term < currentTerm {
		return &pb.AppendEntriesResponse{
			Term:    currentTerm,
			Success: false,
		}
	}

	// If we see a higher term or equal term from a valid leader,
	// update and reset to Follower.
	if req.Term > currentTerm {
		n.state.BecomeFollower(req.Term)
	}

	// Record who the leader is and reset our election timer.
	// This is the core of heartbeat handling — as long as the leader
	// keeps sending AppendEntries, our timer never fires.
	n.state.SetLeader(req.LeaderId)
	n.electionTimer.Reset()

	return &pb.AppendEntriesResponse{
		Term:       n.state.CurrentTerm(),
		Success:    true,
		MatchIndex: n.state.LastLogIndex(),
	}
}
