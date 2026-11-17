// Package application is the Raft protocol core: durable node state, leader
// election with pre-vote, log replication, commit and in-order apply. It
// talks to the network, disk and metrics only through raft/ports.
package application

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nickemma/meridian/internal/raft/domain"
	"github.com/nickemma/meridian/internal/raft/ports"
)

// Node is the top-level Raft node.
// It owns the state machine, election timer, peer transports,
// and the main event loop that drives the consensus protocol.
type Node struct {
	state         *State
	electionTimer *ElectionTimer
	peers         []ports.Peer
	quorumSize    int
	opts          domain.Options
	commitCh      chan uint64   // signals the apply loop when new entries commit
	metrics       ports.Metrics // nil-safe — checked before use
	stateMachine  ports.StateMachine
	applyMu       sync.Mutex
	applyWG       sync.WaitGroup
	// protocolMu serializes state-changing Raft RPCs. RPC handlers run
	// concurrently, but term, vote, and log transitions must be atomic relative
	// to one another: otherwise a follower could grant two votes in one term.
	protocolMu sync.Mutex
	// replicationMu prevents overlapping heartbeat and client-triggered rounds
	// from sending older log prefixes after newer ones to the same follower.
	replicationMu sync.Mutex
	applyWaiters  map[uint64]applyWaiter
	stopCh        chan struct{}
	stopOnce      sync.Once
}

// applyWaiter is a client waiting for its command to be applied. The term
// identifies the command's own entry: by the Log Matching property, a
// different term at the same index means the command was overwritten by
// another leader and was never committed.
type applyWaiter struct {
	term   uint64
	result chan error
}

// NewNode creates a node whose committed entries are applied to stateMachine
// in log-index order. The caller must supply a deterministic machine backed by
// durable storage before exposing client writes. A nil machine applies nothing.
func NewNode(opts domain.Options, state *State, peers []ports.Peer, stateMachine ports.StateMachine) *Node {
	if stateMachine == nil {
		stateMachine = NoopStateMachine{}
	}
	return &Node{
		state:         state,
		electionTimer: NewElectionTimer(opts.ElectionTimeoutMin, opts.ElectionTimeoutMax),
		peers:         append([]ports.Peer(nil), peers...),
		quorumSize:    opts.QuorumSize,
		opts:          opts,
		commitCh:      make(chan uint64, 64),
		stateMachine:  stateMachine,
		applyWaiters:  make(map[uint64]applyWaiter),
		stopCh:        make(chan struct{}),
	}
}

// Run starts the Raft node's main event loop.
func (n *Node) Run(ctx context.Context) {
	log.Printf("[raft] node %s starting — role=%s term=%d",
		n.state.NodeID(), n.state.GetRole(), n.state.CurrentTerm())

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
			if role == domain.Follower || role == domain.Candidate {
				n.runElection()
			}
		}
	}
}

// runHeartbeat sends AppendEntries to all peers on the heartbeat interval.
// On each tick we replicate any pending log entries to followers.
// Empty entries = pure heartbeat. Non-empty = log replication.
func (n *Node) runHeartbeat() {
	ticker := time.NewTicker(n.opts.HeartbeatInterval)
	defer ticker.Stop()

	log.Printf("[raft] %s starting heartbeat ticker", n.state.NodeID())

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
		}

		if n.state.GetRole() != domain.Leader {
			log.Printf("[raft] %s no longer leader, stopping heartbeat",
				n.state.NodeID())
			return
		}
		if n.metrics != nil {
			n.metrics.HeartbeatSent()
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
// locally. It is the primitive the strong client API uses: local append is
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
	command, err := (domain.Command{Type: domain.CommandNoop}).MarshalBinary()
	if err != nil {
		return 0, err
	}
	return n.SubmitAndWait(ctx, command)
}

// Status returns a read-only snapshot for the client status RPC.
func (n *Node) Status() domain.Status {
	return domain.Status{
		NodeID:      n.state.NodeID(),
		Role:        n.state.GetRole().String(),
		Term:        n.state.CurrentTerm(),
		LeaderID:    n.state.LeaderID(),
		CommitIndex: n.state.CommitIndex(),
		LastApplied: n.state.LastApplied(),
	}
}

func (n *Node) submit(command []byte, waiter chan error) (uint64, error) {
	if n.state.GetRole() != domain.Leader {
		return 0, fmt.Errorf("node %s is not the leader (leader is %s)",
			n.state.NodeID(), n.state.LeaderID())
	}

	entry := n.state.AppendEntry(n.state.CurrentTerm(), command)
	if err := n.state.DurabilityError(); err != nil {
		return 0, fmt.Errorf("persist raft entry %d: %w", entry.Index, err)
	}
	if waiter != nil {
		n.applyMu.Lock()
		n.applyWaiters[entry.Index] = applyWaiter{term: entry.Term, result: waiter}
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

// completeApply wakes the client waiting on entry's index. A waiter whose
// command was superseded at that index is told so instead of being told its
// command committed.
func (n *Node) completeApply(entry domain.LogEntry, result error) {
	n.applyMu.Lock()
	waiter, ok := n.applyWaiters[entry.Index]
	if ok {
		delete(n.applyWaiters, entry.Index)
	}
	n.applyMu.Unlock()
	if !ok {
		return
	}
	if waiter.term != entry.Term {
		result = fmt.Errorf("%w: index %d holds a term-%d entry, not the term-%d command", domain.ErrCommandSuperseded, entry.Index, entry.Term, waiter.term)
	}
	waiter.result <- result
}

func (n *Node) removeApplyWaiter(index uint64, target chan error) {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()
	if n.applyWaiters[index].result == target {
		delete(n.applyWaiters, index)
	}
}

// --- Inbound RPCs ---
// Transport adapters call these when peer RPCs arrive.

// RequestVote handles an incoming RequestVote RPC.
func (n *Node) RequestVote(req domain.VoteRequest) domain.VoteResponse {
	defer n.recordMetrics()
	return n.handleRequestVote(req)
}

// AppendEntries handles an incoming heartbeat or replication RPC.
func (n *Node) AppendEntries(req domain.AppendEntriesRequest) domain.AppendEntriesResponse {
	defer n.recordMetrics()
	return n.handleAppendEntries(req)
}

// PreVote handles an incoming PreVote RPC. It changes no durable state.
func (n *Node) PreVote(req domain.PreVoteRequest) domain.PreVoteResponse {
	return n.handlePreVote(req)
}

// SetMetrics wires a metrics sink into the node.
func (n *Node) SetMetrics(m ports.Metrics) {
	n.metrics = m
}

// recordMetrics reports protocol state after a transition.
func (n *Node) recordMetrics() {
	if n.metrics == nil {
		return
	}
	role := n.state.GetRole()
	last := n.state.LastLogIndex()
	sample := ports.Sample{
		Role:        role,
		Term:        n.state.CurrentTerm(),
		CommitIndex: n.state.CommitIndex(),
		LogEntries:  last,
		PeerLag:     make([]ports.PeerLag, 0, len(n.peers)),
	}
	for _, p := range n.peers {
		lag := uint64(0)
		matched := n.state.GetMatchIndex(p.ID())
		if role == domain.Leader && last > matched {
			lag = last - matched
		}
		sample.PeerLag = append(sample.PeerLag, ports.PeerLag{Peer: p.ID(), Entries: lag})
	}
	n.metrics.Observe(sample)
}

// peerIDs returns the IDs of every peer transport.
func (n *Node) peerIDs() []string {
	ids := make([]string, len(n.peers))
	for i, p := range n.peers {
		ids[i] = p.ID()
	}
	return ids
}
