package raft

import (
	"context"
	"fmt"
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
	commitCh      chan uint64 // signals the apply loop when new entries commit
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
		commitCh:   make(chan uint64, 64),
	}
}

// Run starts the Raft node's main event loop.
func (n *Node) Run(ctx context.Context) {
	log.Printf("[raft] node %s starting — role=%s term=%d",
		n.state.NodeID(), n.state.GetRole().string(), n.state.CurrentTerm())

	// Start the apply loop — watches for committed entries.
	go n.runApplyLoop()

	// Start the election timer — begins the countdown to first election
	// if no leader makes contact.
	n.electionTimer.Reset()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[raft] node %s stopping", n.state.NodeID())
			n.electionTimer.Stop()
			close(n.commitCh)
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

// runHeartbeat sends AppendEntries to all peers on the heartbeat interval.
// On each tick we replicate any pending log entries to followers.
// Empty entries = pure heartbeat. Non-empty = log replication.
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

		// Replicate entries to all followers.
		// If there are no new entries this is a pure heartbeat.
		go n.replicateEntries(context.Background())
	}
}

// Submit appends a command to the log and replicates it to followers.
// Returns the log index assigned to this command.
// Returns an error if this node is not the leader.
func (n *Node) Submit(command []byte) (uint64, error) {
	log.Printf("@@@@@@ [SUBMIT DEBUG] node=%s role=%v leader=%s logSize=%d lastIndex=%d",
		n.state.NodeID(),
		n.state.GetRole(),
		n.state.LeaderID(),
		len(n.state.log),
		n.state.LastLogIndex(),
	)
	if n.state.GetRole() != Leader {
		return 0, fmt.Errorf("node %s is not the leader (leader is %s)",
			n.state.NodeID(), n.state.LeaderID())
	}

	entry := n.state.AppendEntry(n.state.CurrentTerm(), command)
	log.Printf("[raft] %s accepted command at index %d",
		n.state.NodeID(), entry.Index)

	// Replicate immediately — don't wait for the next heartbeat tick.
	go n.replicateEntries(context.Background())

	return entry.Index, nil
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

// handles incoming PreVote RPCs. This is part of the pre-vote phase of elections,
func (n *Node) PreVote(
	ctx context.Context, req *pb.PreVoteRequest,
) (*pb.PreVoteResponse, error) {
	return n.handlePreVote(req), nil
}
