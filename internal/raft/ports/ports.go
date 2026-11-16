// Package ports declares what the Raft core needs from the outside world:
// durable state, the replicated state machine, peer transport and metrics.
package ports

import (
	"context"

	"github.com/nickemma/meridian/internal/raft/domain"
	storageports "github.com/nickemma/meridian/internal/storage/ports"
)

// StateStore atomically loads and saves the complete persistent Raft state.
// The interface keeps Raft independent of the file format and permits a later
// log/snapshot store without changing the protocol layer.
type StateStore interface {
	Load() (domain.PersistentState, error)
	Save(domain.PersistentState) error
}

// StateMachine receives committed entries in log-index order. Apply must be
// deterministic: every replica given the same entry sequence must reach the
// same state and result.
type StateMachine interface {
	Apply(index uint64, command []byte) error
}

// KVStore is the durable store the strong KV state machine applies to.
type KVStore = storageports.KVStore

// Peer is the outbound transport to one other node. Implementations bound
// each call with their own short timeout.
type Peer interface {
	ID() string
	RequestVote(ctx context.Context, req domain.VoteRequest) (domain.VoteResponse, error)
	PreVote(ctx context.Context, req domain.PreVoteRequest) (domain.PreVoteResponse, error)
	AppendEntries(ctx context.Context, req domain.AppendEntriesRequest) (domain.AppendEntriesResponse, error)
	Close()
}

// PeerLag is the leader's log index minus a peer's confirmed match index.
type PeerLag struct {
	Peer    string
	Entries uint64
}

// Sample is one snapshot of the node's protocol state for metrics.
type Sample struct {
	Role        domain.Role
	Term        uint64
	CommitIndex uint64
	LogEntries  uint64
	PeerLag     []PeerLag
}

// Metrics receives protocol events. A nil Metrics disables reporting.
type Metrics interface {
	HeartbeatSent()
	ElectionStarted()
	Observe(Sample)
}
