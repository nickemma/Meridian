// Package application is the causal replica service: it serialises access to
// the domain State, persists every transition through a SnapshotStore, and
// offers bounded waits for client dependencies.
package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nickemma/meridian/internal/causal/domain"
	"github.com/nickemma/meridian/internal/causal/ports"
	consistency "github.com/nickemma/meridian/internal/consistency/domain"
)

// Replica is safe for concurrent use. Without a SnapshotStore its state is
// process-local (used by tests and the in-memory protocol checks).
type Replica struct {
	mu          sync.RWMutex
	state       *domain.State
	persistence ports.SnapshotStore
}

func NewReplica(id string) (*Replica, error) {
	state, err := domain.NewState(id)
	if err != nil {
		return nil, err
	}
	return &Replica{state: state}, nil
}

// OpenReplica restores a durable replica snapshot when one exists. A corrupt
// snapshot prevents startup: silently treating it as an empty replica could
// acknowledge writes that violate a client's causal context.
func OpenReplica(id string, persistence ports.SnapshotStore) (*Replica, error) {
	if persistence == nil {
		return nil, fmt.Errorf("causal snapshot store is required")
	}
	replica, err := NewReplica(id)
	if err != nil {
		return nil, err
	}
	replica.persistence = persistence
	encoded, err := persistence.LoadSnapshot()
	if err != nil {
		return nil, err
	}
	if len(encoded) == 0 {
		return replica, nil
	}
	var saved domain.State
	if err := json.Unmarshal(encoded, &saved); err != nil {
		return nil, fmt.Errorf("decode causal snapshot: %w", err)
	}
	if saved.ID != id {
		return nil, fmt.Errorf("causal snapshot belongs to replica %q, not %q", saved.ID, id)
	}
	if err := saved.Validate(); err != nil {
		return nil, err
	}
	replica.state = saved.Clone()
	return replica, nil
}

// Write creates and applies a local causal update. dependencies is normally
// the context returned by an earlier read. It must already be locally visible;
// accepting a write ahead of that context would break read-your-writes and
// transitive causal ordering.
func (r *Replica) Write(key, value []byte, tombstone bool, dependencies consistency.VersionVector, raftIndex, policyVersion uint64) (consistency.Record, error) {
	if len(key) == 0 {
		return consistency.Record{}, fmt.Errorf("key is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.state.DependenciesSatisfied(dependencies, raftIndex) {
		return consistency.Record{}, domain.ErrUnsatisfiedDependencies
	}
	record, err := r.state.NewLocalRecord(key, value, tombstone, dependencies, raftIndex, policyVersion)
	if err != nil {
		return consistency.Record{}, err
	}
	err = r.transition(func(state *domain.State) error { return state.Apply(record) })
	if err != nil {
		return consistency.Record{}, err
	}
	return record.Clone(), nil
}

// WriteContext waits for the dependencies named by a client context to become
// locally visible. It returns the caller's deadline error instead of accepting
// a dependency-violating write when the dependency never arrives.
func (r *Replica) WriteContext(ctx context.Context, key, value []byte, tombstone bool, dependencies consistency.VersionVector, raftIndex, policyVersion uint64) (consistency.Record, error) {
	return retryUntilSatisfied(ctx, func() (consistency.Record, error) {
		return r.Write(key, value, tombstone, dependencies, raftIndex, policyVersion)
	})
}

// Receive accepts a replicated record. Safe records are applied immediately;
// records whose dependencies are missing are retained until a later Receive or
// ObserveRaft makes them safe. Receiving the same record repeatedly is
// idempotent.
func (r *Replica) Receive(record consistency.Record) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := consistency.ValidateRecord(record); err != nil {
		return false, err
	}
	if _, err := record.ID(); err != nil {
		return false, err
	}
	if !r.state.DependenciesSatisfied(record.Dependencies, record.RaftIndex) {
		return false, r.transition(func(state *domain.State) error { return state.Hold(record) })
	}
	err := r.transition(func(state *domain.State) error {
		if err := state.Apply(record); err != nil {
			return err
		}
		state.Drain()
		return nil
	})
	return err == nil, err
}

// ObserveRaft advances the local strong-state watermark after the corresponding
// Raft entry has been applied to local storage. It is intentionally separate
// from Receive: merely receiving a causal record that depends on a Raft index
// does not mean the local strong state machine has applied that index.
func (r *Replica) ObserveRaft(index uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index <= r.state.AppliedRaftIdx {
		return nil
	}
	return r.transition(func(state *domain.State) error {
		state.AppliedRaftIdx = index
		state.Drain()
		return nil
	})
}

func (r *Replica) Read(key []byte, required consistency.VersionVector, raftIndex uint64) (domain.Snapshot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state.Read(key, required, raftIndex)
}

// ReadContext waits for required causal and Raft dependencies to become
// locally visible. It is deliberately a bounded wait: success never exposes a
// value ahead of a named dependency, and expiry is visible to the caller.
func (r *Replica) ReadContext(ctx context.Context, key []byte, required consistency.VersionVector, raftIndex uint64) (domain.Snapshot, error) {
	return retryUntilSatisfied(ctx, func() (domain.Snapshot, error) {
		return r.Read(key, required, raftIndex)
	})
}

// Pending reports how many received records await dependencies.
func (r *Replica) Pending() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.state.Pending)
}

// DependenciesSatisfied reports whether a request can run without waiting. It
// does not clone registers or expose any values to the instrumentation layer.
func (r *Replica) DependenciesSatisfied(required consistency.VersionVector, raftIndex uint64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state.DependenciesSatisfied(required, raftIndex)
}

// transition applies change and persists the result. If either step fails the
// previous state is restored, so memory never runs ahead of the snapshot.
// Caller must hold r.mu for writing.
func (r *Replica) transition(change func(*domain.State) error) error {
	previous := r.state.Clone()
	if err := change(r.state); err != nil {
		r.state = previous
		return err
	}
	if r.persistence == nil {
		return nil
	}
	encoded, err := json.Marshal(r.state)
	if err == nil {
		err = r.persistence.SaveSnapshot(encoded)
	}
	if err != nil {
		r.state = previous
		return fmt.Errorf("persist causal replica: %w", err)
	}
	return nil
}

// retryUntilSatisfied polls attempt until it stops reporting unsatisfied
// dependencies or ctx ends.
func retryUntilSatisfied[T any](ctx context.Context, attempt func() (T, error)) (T, error) {
	for {
		result, err := attempt()
		if !errors.Is(err, domain.ErrUnsatisfiedDependencies) {
			return result, err
		}
		select {
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}
