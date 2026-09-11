package raft

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/nickemma/meridian/internal/config"
	"github.com/nickemma/meridian/internal/metrics"
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
	commitCh      chan uint64      // signals the apply loop when new entries commit
	metrics       *metrics.Metrics // nil-safe — checked before use
	stateMachine  StateMachine
	applyMu       sync.Mutex
	applyWG       sync.WaitGroup
	// protocolMu serializes state-changing Raft RPCs. gRPC dispatches handlers
	// concurrently, but term, vote, and log transitions must be atomic relative
	// to one another: otherwise a follower could grant two votes in one term.
	protocolMu sync.Mutex
	// replicationMu prevents overlapping heartbeat and client-triggered rounds
	// from sending older log prefixes after newer ones to the same follower.
	replicationMu sync.Mutex
	applyWaiters  map[uint64]chan error
	stopCh        chan struct{}
	stopOnce      sync.Once
}

// NewNode creates a new Raft node from config.
func NewNode(cfg *config.Config) *Node {
	return NewNodeWithStateMachine(cfg, NoopStateMachine{})
}

// NewNodeWithStateMachine creates a Raft node whose committed entries are
// applied to stateMachine in log-index order. The caller must supply a
// deterministic machine backed by durable storage before exposing client writes.
func NewNodeWithStateMachine(cfg *config.Config, stateMachine StateMachine) *Node {
	return newNode(cfg, stateMachine, NewState(cfg.NodeID))
}

// NewDurableNode creates the node used by the server. It restores the Raft
// state before opening peer connections; a state-store error is fatal to node
// startup because participating with a fresh term or truncated log is unsafe.
func NewDurableNode(cfg *config.Config, stateMachine StateMachine) (*Node, error) {
	store, err := NewFileStateStore(filepath.Join(cfg.DataDir, "raft"))
	if err != nil {
		return nil, err
	}
	state, err := NewStateFromStore(cfg.NodeID, store)
	if err != nil {
		return nil, err
	}
	return newNode(cfg, stateMachine, state), nil
}

func newNode(cfg *config.Config, stateMachine StateMachine, state *State) *Node {
	if stateMachine == nil {
		stateMachine = NoopStateMachine{}
	}
	peers := make([]*PeerClient, len(cfg.Peers))
	for i, p := range cfg.Peers {
		peers[i] = NewPeerClient(p.ID, p.Address)
	}

	return &Node{
		state: state,
		electionTimer: NewElectionTimer(
			cfg.ElectionTimeoutMin,
			cfg.ElectionTimeoutMax,
		),
		peers:        peers,
		quorumSize:   cfg.QuorumSize,
		cfg:          cfg,
		commitCh:     make(chan uint64, 64),
		metrics:      nil,
		stateMachine: stateMachine,
		applyWaiters: make(map[uint64]chan error),
		stopCh:       make(chan struct{}),
	}
}

// Run starts the Raft node's main event loop.
func (n *Node) Run(ctx context.Context) {
	log.Printf("[raft] node %s starting — role=%s term=%d",
		n.state.NodeID(), n.state.GetRole().string(), n.state.CurrentTerm())

	// Start the apply loop — watches for committed entries.
	n.applyWG.Add(1)
	go func() {
		defer n.applyWG.Done()
		n.runApplyLoop()
	}()
	if n.state.CommitIndex() > n.state.LastApplied() {
		n.notifyApply(n.state.CommitIndex())
	}

	// Start the election timer — begins the countdown to first election
	// if no leader makes contact.
	n.electionTimer.Reset()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[raft] node %s stopping", n.state.NodeID())
			n.electionTimer.Stop()
			n.stopOnce.Do(func() { close(n.stopCh) })
			n.applyWG.Wait()
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
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
		}

		if n.state.GetRole() != Leader {
			log.Printf("[raft] %s no longer leader, stopping heartbeat",
				n.state.NodeID())
			return
		}
		if n.metrics != nil {
			n.metrics.RaftHeartbeatsTotal.Inc()
		}
		n.recordMetrics()

		// Replicate entries to all followers.
		// If there are no new entries this is a pure heartbeat.
		go n.replicateEntries(context.Background())
	}
}

// Submit appends a command to the log and replicates it to followers.
// Returns the log index assigned to this command.
// Returns an error if this node is not the leader.
func (n *Node) Submit(command []byte) (uint64, error) {
	return n.submit(command, nil)
}

// SubmitAndWait returns only after a leader has committed and applied command
// locally. It is the primitive the strong client API will use: local append is
// intentionally not reported as a successful write.
func (n *Node) SubmitAndWait(ctx context.Context, command []byte) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	waiter := make(chan error, 1)
	index, err := n.submit(command, waiter)
	if err != nil {
		return 0, err
	}

	select {
	case err := <-waiter:
		return index, err
	case <-ctx.Done():
		n.removeApplyWaiter(index, waiter)
		return index, ctx.Err()
	}
}

// LinearizableBarrier commits and applies a no-op entry in the leader's current
// term. Until the lower-latency ReadIndex protocol is added, this is the safe
// strong-read barrier: a subsequent local state-machine read observes every
// command committed before the barrier.
func (n *Node) LinearizableBarrier(ctx context.Context) (uint64, error) {
	command, err := (Command{Type: CommandNoop}).MarshalBinary()
	if err != nil {
		return 0, err
	}
	return n.SubmitAndWait(ctx, command)
}

// Status is a read-only Raft node snapshot for the client status RPC.
type Status struct {
	NodeID      string
	Role        string
	Term        uint64
	LeaderID    string
	CommitIndex uint64
	LastApplied uint64
}

func (n *Node) Status() Status {
	return Status{
		NodeID:      n.state.NodeID(),
		Role:        n.state.GetRole().String(),
		Term:        n.state.CurrentTerm(),
		LeaderID:    n.state.LeaderID(),
		CommitIndex: n.state.CommitIndex(),
		LastApplied: n.state.LastApplied(),
	}
}

func (n *Node) submit(command []byte, waiter chan error) (uint64, error) {
	if n.state.GetRole() != Leader {
		return 0, fmt.Errorf("node %s is not the leader (leader is %s)",
			n.state.NodeID(), n.state.LeaderID())
	}

	entry := n.state.AppendEntry(n.state.CurrentTerm(), command)
	if err := n.state.DurabilityError(); err != nil {
		return 0, fmt.Errorf("persist raft entry %d: %w", entry.Index, err)
	}
	if waiter != nil {
		n.applyMu.Lock()
		n.applyWaiters[entry.Index] = waiter
		n.applyMu.Unlock()
	}
	log.Printf("[raft] %s accepted command at index %d", n.state.NodeID(), entry.Index)

	// Replicate immediately — don't wait for the next heartbeat tick.
	go n.replicateEntries(context.Background())

	return entry.Index, nil
}

func (n *Node) notifyApply(index uint64) {
	select {
	case <-n.stopCh:
		return
	default:
	}
	select {
	case <-n.stopCh:
		return
	case n.commitCh <- index:
	default:
		// The apply loop always catches up to the durable commit index.
	}
}

func (n *Node) completeApply(index uint64, result error) {
	n.applyMu.Lock()
	waiter, ok := n.applyWaiters[index]
	if ok {
		delete(n.applyWaiters, index)
	}
	n.applyMu.Unlock()
	if ok {
		waiter <- result
	}
}

func (n *Node) removeApplyWaiter(index uint64, target chan error) {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()
	if n.applyWaiters[index] == target {
		delete(n.applyWaiters, index)
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

// handles incoming PreVote RPCs. This is part of the pre-vote phase of elections,
func (n *Node) PreVote(
	ctx context.Context, req *pb.PreVoteRequest,
) (*pb.PreVoteResponse, error) {
	return n.handlePreVote(req), nil
}

// SetMetrics wires Prometheus metrics into the Raft node.
func (n *Node) SetMetrics(m *metrics.Metrics) {
	n.metrics = m
}

// recordMetrics updates Prometheus gauges from current state.
// Called after every state transition.
func (n *Node) recordMetrics() {
	if n.metrics == nil {
		return
	}
	n.metrics.RaftTerm.Set(float64(n.state.CurrentTerm()))
	n.metrics.RaftCommitIndex.Set(float64(n.state.CommitIndex()))
	n.metrics.RaftLogEntries.Set(float64(n.state.LastLogIndex()))

	switch n.state.GetRole() {
	case Follower:
		n.metrics.RaftRole.Set(0)
	case Candidate:
		n.metrics.RaftRole.Set(1)
	case Leader:
		n.metrics.RaftRole.Set(2)
	}
}
