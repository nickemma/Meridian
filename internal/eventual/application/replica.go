// Package application is the eventual replica service: serialised access to
// the domain State, one durable checkpoint per transition, and the periodic
// anti-entropy loop.
package application

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	consistency "github.com/nickemma/meridian/internal/consistency/domain"
	"github.com/nickemma/meridian/internal/eventual/domain"
	"github.com/nickemma/meridian/internal/eventual/ports"
)

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

func OpenReplica(id string, persistence ports.SnapshotStore) (*Replica, error) {
	if persistence == nil {
		return nil, fmt.Errorf("eventual snapshot store is required")
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
		return nil, fmt.Errorf("decode eventual snapshot: %w", err)
	}
	if err := saved.Validate(id); err != nil {
		return nil, err
	}
	replica.state = saved.Clone()
	return replica, nil
}

// Write applies an asynchronous local update.
func (r *Replica) Write(key, value []byte, tombstone bool, policyVersion uint64) (consistency.Record, error) {
	if len(key) == 0 {
		return consistency.Record{}, fmt.Errorf("key is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	record, err := r.state.NewLocalRecord(key, value, tombstone, policyVersion)
	if err != nil {
		return consistency.Record{}, err
	}
	if err := r.transition(func(state *domain.State) error { return state.Apply(record) }); err != nil {
		return consistency.Record{}, err
	}
	return record.Clone(), nil
}

// Receive is idempotent and order-independent.
func (r *Replica) Receive(record consistency.Record) error {
	return r.ReceiveAll([]consistency.Record{record})
}

// ReceiveAll merges one anti-entropy batch as a single durable transition.
// Periodic gossip carries every currently maximal record. Checkpointing after
// each element would make a full-state round quadratic in storage work and can
// starve the Raft RPC service that shares the process.
func (r *Replica) ReceiveAll(records []consistency.Record) error {
	for _, record := range records {
		if err := domain.ValidateIncoming(record); err != nil {
			return err
		}
	}
	if len(records) == 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.transition(func(state *domain.State) error {
		for _, record := range records {
			if err := state.Apply(record); err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *Replica) Read(key []byte) domain.Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state.Read(key)
}

// Records returns all maximal records in a deterministic order. Periodic gossip
// may resend them safely; a receiver deduplicates by vector clock.
func (r *Replica) Records() []consistency.Record {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state.Records()
}

// RunAntiEntropy gossips the full maximal state every interval until ctx
// ends. It recovers any update whose initial push was missed.
func (r *Replica) RunAntiEntropy(ctx context.Context, interval time.Duration, gossip ports.Gossip) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			gossip(r.Records())
		}
	}
}

// transition applies change and persists the result, restoring the previous
// state if either fails. Caller must hold r.mu for writing.
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
		return fmt.Errorf("persist eventual replica: %w", err)
	}
	return nil
}
