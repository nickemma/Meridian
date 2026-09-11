// Package eventual implements the convergent multi-value-register portion of
// Meridian's asynchronous gossip path. It never waits for a causal dependency:
// callers that require that guarantee must use the causal path instead.
package eventual

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/nickemma/meridian/internal/consistency"
)

type Snapshot struct {
	Values   []consistency.Record
	Frontier consistency.VersionVector
}

type Replica struct {
	mu          sync.RWMutex
	id          string
	frontier    consistency.VersionVector
	registers   map[string][]consistency.Record
	persistence SnapshotStore
}

type SnapshotStore interface {
	LoadSnapshot() ([]byte, error)
	SaveSnapshot([]byte) error
}

func NewReplica(id string) (*Replica, error) {
	if id == "" {
		return nil, fmt.Errorf("replica ID is required")
	}
	return &Replica{id: id, frontier: make(consistency.VersionVector), registers: make(map[string][]consistency.Record)}, nil
}

func OpenReplica(id string, persistence SnapshotStore) (*Replica, error) {
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
	var saved persistedState
	if err := json.Unmarshal(encoded, &saved); err != nil {
		return nil, fmt.Errorf("decode eventual snapshot: %w", err)
	}
	if saved.ID != id || saved.Frontier == nil || saved.Registers == nil {
		return nil, fmt.Errorf("invalid eventual snapshot for replica %q", id)
	}
	for key, records := range saved.Registers {
		for _, record := range records {
			if err := consistency.ValidateRecord(record); err != nil || key != string(record.Key) {
				return nil, fmt.Errorf("invalid persisted eventual register %q", key)
			}
		}
	}
	replica.restoreLocked(saved)
	return replica, nil
}

// Write applies an asynchronous local update. Its version is based on every
// update this replica has observed, making a later write supersede observed
// values while preserving concurrent values from disconnected replicas.
func (r *Replica) Write(key, value []byte, tombstone bool, policyVersion uint64) (consistency.Record, error) {
	if len(key) == 0 {
		return consistency.Record{}, fmt.Errorf("key is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	version, err := r.frontier.Increment(r.id)
	if err != nil {
		return consistency.Record{}, err
	}
	record := consistency.Record{
		Key:           bytes.Clone(key),
		Value:         bytes.Clone(value),
		Tombstone:     tombstone,
		Version:       version,
		Dependencies:  consistency.VersionVector{},
		Origin:        r.id,
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

// Receive is idempotent and order-independent. MergeRegister is commutative
// over the set of received versions, so eventually every connected replica
// reaches the same maximal sibling set.
func (r *Replica) Receive(record consistency.Record) error {
	if err := consistency.ValidateRecord(record); err != nil {
		return err
	}
	if len(record.Dependencies) != 0 {
		return fmt.Errorf("eventual record must not carry causal dependencies")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	previous := r.snapshotLocked()
	if err := r.applyLocked(record); err != nil {
		return err
	}
	return r.checkpointOrRestoreLocked(previous)
}

func (r *Replica) Read(key []byte) Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	values := r.registers[string(key)]
	result := Snapshot{Values: make([]consistency.Record, len(values)), Frontier: r.frontier.Clone()}
	for index, value := range values {
		result.Values[index] = clone(value)
	}
	return result
}

// Records returns all maximal records in a deterministic order. Periodic gossip
// may resend them safely; a receiver deduplicates by vector clock.
func (r *Replica) Records() []consistency.Record {
	r.mu.RLock()
	defer r.mu.RUnlock()
	keys := make([]string, 0, len(r.registers))
	for key := range r.registers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var records []consistency.Record
	for _, key := range keys {
		for _, record := range r.registers[key] {
			records = append(records, clone(record))
		}
	}
	return records
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
	ID        string                          `json:"id"`
	Frontier  consistency.VersionVector       `json:"frontier"`
	Registers map[string][]consistency.Record `json:"registers"`
}

func (r *Replica) snapshotLocked() persistedState {
	state := persistedState{ID: r.id, Frontier: r.frontier.Clone(), Registers: make(map[string][]consistency.Record, len(r.registers))}
	for key, records := range r.registers {
		cloned := make([]consistency.Record, len(records))
		for index, record := range records {
			cloned[index] = clone(record)
		}
		state.Registers[key] = cloned
	}
	return state
}

func (r *Replica) restoreLocked(state persistedState) {
	r.frontier = state.Frontier.Clone()
	r.registers = make(map[string][]consistency.Record, len(state.Registers))
	for key, records := range state.Registers {
		cloned := make([]consistency.Record, len(records))
		for index, record := range records {
			cloned[index] = clone(record)
		}
		r.registers[key] = cloned
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
		return fmt.Errorf("persist eventual replica: %w", err)
	}
	return nil
}
