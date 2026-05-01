package raft

import (
	"context"
	"log"
	"time"

	"github.com/nickemma/meridian/internal/config"
	pb "github.com/nickemma/meridian/proto/raft"
)

// Node is the top-level Raft node.
// It owns the state machine, election timer, peer clients,
// and the main event loop that drives the consensus protocol.
type Node struct {
	pb.UnimplementedRaftServiceServer

	state         *State
	electionTimer *ElectionTimer
	peers         []*PeerClient
	quorumSize    int
	cfg           *config.Config
}

// NewNode creates a new Raft node from config.
func NewNode(cfg *config.Config) *Node {
	peers := make([]*PeerClient, len(cfg.Peers))
	for i, p := range cfg.Peers {
		peers[i] = NewPeerClient(p.ID, p.Address)
	}

	return &Node{
		state: NewState(cfg.NodeID),
		electionTimer: NewElectionTimer(
			cfg.ElectionTimeoutMin,
			cfg.ElectionTimeoutMax,
		),
		peers:      peers,
		quorumSize: cfg.QuorumSize,
		cfg:        cfg,
	}
}

// Run starts the Raft node's main event loop.
// Blocks until ctx is cancelled.
func (n *Node) Run(ctx context.Context) {
	log.Printf("[raft] node %s starting — role=%s term=%d",
		n.state.NodeID(), n.state.GetRole().string(), n.state.CurrentTerm())

	// Start the election timer — begins the countdown to first election
	// if no leader makes contact.
	n.electionTimer.Reset()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[raft] node %s stopping", n.state.NodeID())
			n.electionTimer.Stop()
			for _, p := range n.peers {
				p.Close()
			}
			return

		case <-n.electionTimer.Fired():
			// Timer fired — no heartbeat received from leader.
			// Start an election if we are a Follower or Candidate.
			// Leaders never have their election timer running.
			role := n.state.GetRole()
			if role == Follower || role == Candidate {
				n.runElection()
			}
		}
	}
}

// runHeartbeat sends AppendEntries RPCs to all peers on the heartbeat interval.
// Called in a goroutine when this node becomes Leader.
// Exits when this node is no longer Leader or ctx is cancelled.
//
// AppendEntries with empty entries = heartbeat.
// This tells followers the leader is alive and resets their election timers.
// Full implementation with log replication comes in Phase 3d.
func (n *Node) runHeartbeat() {
	ticker := time.NewTicker(n.cfg.HeartbeatInterval)
	defer ticker.Stop()

	log.Printf("[raft] %s starting heartbeat ticker", n.state.NodeID())

	for {
		<-ticker.C

		if n.state.GetRole() != Leader {
			log.Printf("[raft] %s no longer leader, stopping heartbeat",
				n.state.NodeID())
			return
		}

		for _, peer := range n.peers {
			go n.sendHeartbeat(peer)
		}
	}
}

// sendHeartbeat sends a single empty AppendEntries to one peer.
func (n *Node) sendHeartbeat(peer *PeerClient) {
	req := &pb.AppendEntriesRequest{
		Term:         n.state.CurrentTerm(),
		LeaderId:     n.state.NodeID(),
		PrevLogIndex: n.state.LastLogIndex(),
		PrevLogTerm:  n.state.LastLogTerm(),
		Entries:      nil, // empty = heartbeat
		LeaderCommit: n.state.CommitIndex(),
	}

	resp, err := peer.AppendEntries(context.Background(), req)
	if err != nil {
		// Peer unreachable — log and move on.
		// We will retry on the next heartbeat tick.
		log.Printf("[raft] %s heartbeat to %s failed: %v",
			n.state.NodeID(), peer.ID(), err)
		return
	}

	// If peer has a higher term we are a stale leader — step down.
	if resp.Term > n.state.CurrentTerm() {
		log.Printf("[raft] %s saw higher term %d from %s, stepping down",
			n.state.NodeID(), resp.Term, peer.ID())
		n.state.BecomeFollower(resp.Term)
		n.electionTimer.Reset()
	}
}

// --- gRPC handler implementations ---
// These satisfy the pb.RaftServiceServer interface.
// The gRPC server calls these when RPCs arrive.

// RequestVote handles an incoming RequestVote RPC.
func (n *Node) RequestVote(
	ctx context.Context,
	req *pb.RequestVoteRequest,
) (*pb.RequestVoteResponse, error) {
	return n.handleRequestVote(req), nil
}

// AppendEntries handles an incoming AppendEntries RPC.
// Full implementation comes in Phase 3d — for now we accept heartbeats.
func (n *Node) AppendEntries(
	ctx context.Context,
	req *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {
	return n.handleAppendEntries(req), nil
}
