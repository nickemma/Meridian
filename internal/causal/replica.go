// Package causal implements the in-memory ordering core of Meridian's causal
// replication path. Transport and durable storage deliberately sit outside this
// package: a replica decides when a received record is safe to expose, and its
// caller persists or disseminates records after that decision.
package causal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nickemma/meridian/internal/consistency"
)

var (
	// ErrUnsatisfiedDependencies means a client context refers to an update the
	// local replica has not applied. Callers may wait, route elsewhere, or fail
	// the request; they must not create or expose an update ahead of it.
	ErrUnsatisfiedDependencies = errors.New("causal dependencies are not locally satisfied")
	ErrInvalidReplicaID        = errors.New("replica ID is required")
)

// Snapshot is a stable view returned to a client. Values has one member for a
// resolved register and multiple members when concurrent writes are visible.
// Returning siblings instead of choosing one by timestamp prevents the causal
// path from silently discarding a conflict.
type Snapshot struct {
	Values         []consistency.Record
	Frontier       consistency.VersionVector
	AppliedRaftIdx uint64
}

// Replica holds a causally delivered frontier and multi-value registers. Its
// state is process-local for now; the next layer persists applied records and
// transports Receive calls between replicas.
type Replica struct {
	mu             sync.RWMutex
	id             string
	frontier       consistency.VersionVector
	appliedRaftIdx uint64
	registers      map[string][]consistency.Record
	pending        map[string]consistency.Record
	persistence    SnapshotStore
}

// SnapshotStore stores one complete, self-validating replica snapshot. It is
// intentionally structural so storage adapters can live outside this package.
type SnapshotStore interface {
	LoadSnapshot() ([]byte, error)
	SaveSnapshot([]byte) error
}

func NewReplica(id string) (*Replica, error) {
	if id == "" {
		return nil, ErrInvalidReplicaID
	}
	return &Replica{
		id:        id,
		frontier:  make(consistency.VersionVector),
		registers: make(map[string][]consistency.Record),
		pending:   make(map[string]consistency.Record),
	}, nil
}

// OpenReplica restores a durable replica snapshot when one exists. A corrupt
// snapshot prevents startup: silently treating it as an empty replica could
// acknowledge writes that violate a client's causal context.
func OpenReplica(id string, persistence SnapshotStore) (*Replica, error) {
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
	var saved persistedState
	if err := json.Unmarshal(encoded, &saved); err != nil {
		return nil, fmt.Errorf("decode causal snapshot: %w", err)
	}
	if saved.ID != id {
		return nil, fmt.Errorf("causal snapshot belongs to replica %q, not %q", saved.ID, id)
	}
	if err := validatePersistedState(saved); err != nil {
		return nil, err
	}
	replica.restoreLocked(saved)
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
	if !r.dependenciesSatisfiedLocked(dependencies, raftIndex) {
		return consistency.Record{}, ErrUnsatisfiedDependencies
	}
	version, err := r.frontier.Merge(dependencies).Increment(r.id)
	if err != nil {
		return consistency.Record{}, err
	}
	record := consistency.Record{
		Key:           bytes.Clone(key),
		Value:         bytes.Clone(value),
		Tombstone:     tombstone,
		Version:       version,
		Dependencies:  dependencies.Clone(),
		Origin:        r.id,
		RaftIndex:     raftIndex,
		PolicyVersion: policyVersion,
	}
	previous := r.snapshotLocked()
	if err := r.applyLocked(record); err != nil {
		return consistency.Record{}, err
	}
	if err := r.checkpointOrRestoreLocked(previous); err != nil {
		return consistency.Record{}, err
	}
	return clone(record), nil
}

// WriteContext waits for the dependencies named by a client context to become
// locally visible. It returns the caller's deadline error instead of accepting
// a dependency-violating write when the dependency never arrives.
func (r *Replica) WriteContext(ctx context.Context, key, value []byte, tombstone bool, dependencies consistency.VersionVector, raftIndex, policyVersion uint64) (consistency.Record, error) {
	for {
		record, err := r.Write(key, value, tombstone, dependencies, raftIndex, policyVersion)
		if !errors.Is(err, ErrUnsatisfiedDependencies) {
			return record, err
		}
		if err := waitForContext(ctx); err != nil {
			return consistency.Record{}, err
		}
	}
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
	key, err := recordID(record)
	if err != nil {
		return false, err
	}
	if !r.dependenciesSatisfiedLocked(record.Dependencies, record.RaftIndex) {
		previous := r.snapshotLocked()
		r.pending[key] = clone(record)
		if err := r.checkpointOrRestoreLocked(previous); err != nil {
			return false, err
		}
		return false, nil
	}
	previous := r.snapshotLocked()
	if err := r.applyLocked(record); err != nil {
		return false, err
	}
	r.drainLocked()
	if err := r.checkpointOrRestoreLocked(previous); err != nil {
		return false, err
	}
	return true, nil
}

// ObserveRaft advances the local strong-state watermark after the corresponding
// Raft entry has been applied to local storage. It is intentionally separate
// from Receive: merely receiving a causal record that depends on a Raft index
// does not mean the local strong state machine has applied that index.
func (r *Replica) ObserveRaft(index uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index > r.appliedRaftIdx {
		previous := r.snapshotLocked()
		r.appliedRaftIdx = index
		r.drainLocked()
		if err := r.checkpointOrRestoreLocked(previous); err != nil {
			return err
		}
	}
	return nil
}

func (r *Replica) Read(key []byte, required consistency.VersionVector, raftIndex uint64) (Snapshot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.dependenciesSatisfiedLocked(required, raftIndex) {
		return Snapshot{}, ErrUnsatisfiedDependencies
	}
	values := r.registers[string(key)]
	result := Snapshot{
		Values:         make([]consistency.Record, len(values)),
		Frontier:       r.frontier.Clone(),
		AppliedRaftIdx: r.appliedRaftIdx,
	}
	for index, value := range values {
		result.Values[index] = clone(value)
	}
	return result, nil
}

// ReadContext waits for required causal and Raft dependencies to become
// locally visible. It is deliberately a bounded wait: success never exposes a
// value ahead of a named dependency, and expiry is visible to the caller.
func (r *Replica) ReadContext(ctx context.Context, key []byte, required consistency.VersionVector, raftIndex uint64) (Snapshot, error) {
	for {
		snapshot, err := r.Read(key, required, raftIndex)
		if !errors.Is(err, ErrUnsatisfiedDependencies) {
			return snapshot, err
		}
		if err := waitForContext(ctx); err != nil {
			return Snapshot{}, err
		}
	}
}

func waitForContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Millisecond):
		return nil
	}
}

func (r *Replica) Pending() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.pending)
}

func (r *Replica) dependenciesSatisfiedLocked(dependencies consistency.VersionVector, raftIndex uint64) bool {
	return r.frontier.Dominates(dependencies) && r.appliedRaftIdx >= raftIndex
}

func (r *Replica) applyLocked(record consistency.Record) error {
	merged, err := consistency.MergeRegister(r.registers[string(record.Key)], record)
	if err != nil {
		return err
	}
	r.registers[string(record.Key)] = merged
	r.frontier = r.frontier.Merge(record.Version)
	return nil
}

func (r *Replica) drainLocked() {
	for progressed := true; progressed; {
		progressed = false
		for id, record := range r.pending {
			if !r.dependenciesSatisfiedLocked(record.Dependencies, record.RaftIndex) {
				continue
			}
			if err := r.applyLocked(record); err != nil {
				// Receive validates records before enqueueing. Leaving a malformed
				// record pending would make future retries non-deterministic; delete
				// it rather than allowing it to block unrelated records forever.
				delete(r.pending, id)
				continue
			}
			delete(r.pending, id)
			progressed = true
		}
	}
}

func recordID(record consistency.Record) (string, error) {
	version, err := record.Version.MarshalBinary()
	if err != nil {
		return "", err
	}
	return string(record.Key) + "\x00" + string(version), nil
}

func clone(record consistency.Record) consistency.Record {
	return consistency.Record{
		Key:           bytes.Clone(record.Key),
		Value:         bytes.Clone(record.Value),
		Tombstone:     record.Tombstone,
		Version:       record.Version.Clone(),
		Dependencies:  record.Dependencies.Clone(),
		Origin:        record.Origin,
		RaftIndex:     record.RaftIndex,
		PolicyVersion: record.PolicyVersion,
	}
}

type persistedState struct {
	ID             string                          `json:"id"`
	Frontier       consistency.VersionVector       `json:"frontier"`
	AppliedRaftIdx uint64                          `json:"applied_raft_index"`
	Registers      map[string][]consistency.Record `json:"registers"`
	Pending        map[string]consistency.Record   `json:"pending"`
}

func (r *Replica) snapshotLocked() persistedState {
	state := persistedState{ID: r.id, Frontier: r.frontier.Clone(), AppliedRaftIdx: r.appliedRaftIdx, Registers: make(map[string][]consistency.Record, len(r.registers)), Pending: make(map[string]consistency.Record, len(r.pending))}
	for key, records := range r.registers {
		cloned := make([]consistency.Record, len(records))
		for index, record := range records {
			cloned[index] = clone(record)
		}
		state.Registers[key] = cloned
	}
	for key, record := range r.pending {
		state.Pending[key] = clone(record)
	}
	return state
}

func (r *Replica) restoreLocked(state persistedState) {
	r.frontier = state.Frontier.Clone()
	r.appliedRaftIdx = state.AppliedRaftIdx
	r.registers = make(map[string][]consistency.Record, len(state.Registers))
	for key, records := range state.Registers {
		cloned := make([]consistency.Record, len(records))
		for index, record := range records {
			cloned[index] = clone(record)
		}
		r.registers[key] = cloned
	}
	r.pending = make(map[string]consistency.Record, len(state.Pending))
	for key, record := range state.Pending {
		r.pending[key] = clone(record)
	}
}

func (r *Replica) checkpointOrRestoreLocked(previous persistedState) error {
	if r.persistence == nil {
		return nil
	}
	encoded, err := json.Marshal(r.snapshotLocked())
	if err == nil {
		err = r.persistence.SaveSnapshot(encoded)
	}
	if err != nil {
		r.restoreLocked(previous)
		return fmt.Errorf("persist causal replica: %w", err)
	}
	return nil
}

func validatePersistedState(state persistedState) error {
	if state.ID == "" {
		return fmt.Errorf("causal snapshot has no replica ID")
	}
	if state.Frontier == nil || state.Registers == nil || state.Pending == nil {
		return fmt.Errorf("causal snapshot is incomplete")
	}
	for key, records := range state.Registers {
		for _, record := range records {
			if err := consistency.ValidateRecord(record); err != nil {
				return fmt.Errorf("invalid persisted causal register %q: %w", key, err)
			}
			if key != string(record.Key) {
				return fmt.Errorf("persisted causal register key %q does not match record key", key)
			}
		}
	}
	for key, record := range state.Pending {
		id, err := recordID(record)
		if err != nil || id != key {
			return fmt.Errorf("invalid persisted causal pending record %q", key)
		}
	}
	return nil
}
