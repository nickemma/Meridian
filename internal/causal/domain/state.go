// Package domain is the ordering core of Meridian's causal replication path.
// A State decides when a received record is safe to expose; it knows nothing
// about locks, persistence or transport.
package domain

import (
	"errors"
	"fmt"

	consistency "github.com/nickemma/meridian/internal/consistency/domain"
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

// State is a causally delivered frontier, the multi-value registers it makes
// visible, and records still waiting for dependencies. Its JSON form is the
// durable snapshot format.
type State struct {
	ID             string                        `json:"id"`
	Frontier       consistency.VersionVector     `json:"frontier"`
	AppliedRaftIdx uint64                        `json:"applied_raft_index"`
	Registers      consistency.Registers         `json:"registers"`
	Pending        map[string]consistency.Record `json:"pending"`
}

func NewState(id string) (*State, error) {
	if id == "" {
		return nil, ErrInvalidReplicaID
	}
	return &State{
		ID:        id,
		Frontier:  make(consistency.VersionVector),
		Registers: make(consistency.Registers),
		Pending:   make(map[string]consistency.Record),
	}, nil
}

// DependenciesSatisfied reports whether both the vector and the strong-path
// (Raft index) dependencies are locally visible.
func (s *State) DependenciesSatisfied(dependencies consistency.VersionVector, raftIndex uint64) bool {
	return s.Frontier.Dominates(dependencies) && s.AppliedRaftIdx >= raftIndex
}

// NewLocalRecord creates the record for a local write whose dependencies are
// already satisfied. Its version follows every update this replica observed.
func (s *State) NewLocalRecord(key, value []byte, tombstone bool, dependencies consistency.VersionVector, raftIndex, policyVersion uint64) (consistency.Record, error) {
	version, err := s.Frontier.Merge(dependencies).Increment(s.ID)
	if err != nil {
		return consistency.Record{}, err
	}
	return consistency.Record{
		Key:           key,
		Value:         value,
		Tombstone:     tombstone,
		Version:       version,
		Dependencies:  dependencies,
		Origin:        s.ID,
		RaftIndex:     raftIndex,
		PolicyVersion: policyVersion,
	}.Clone(), nil
}

// Apply makes record visible and advances the frontier.
func (s *State) Apply(record consistency.Record) error {
	if err := s.Registers.Apply(record); err != nil {
		return err
	}
	s.Frontier = s.Frontier.Merge(record.Version)
	return nil
}

// Hold queues a validated record until its dependencies arrive.
func (s *State) Hold(record consistency.Record) error {
	id, err := record.ID()
	if err != nil {
		return err
	}
	s.Pending[id] = record.Clone()
	return nil
}

// Drain applies every pending record whose dependencies are now satisfied,
// repeating until no further record becomes safe.
func (s *State) Drain() {
	for progressed := true; progressed; {
		progressed = false
		for id, record := range s.Pending {
			if !s.DependenciesSatisfied(record.Dependencies, record.RaftIndex) {
				continue
			}
			if err := s.Apply(record); err != nil {
				// Receive validates records before enqueueing. Leaving a malformed
				// record pending would make future retries non-deterministic; delete
				// it rather than allowing it to block unrelated records forever.
				delete(s.Pending, id)
				continue
			}
			delete(s.Pending, id)
			progressed = true
		}
	}
}

// Read returns key's visible values, failing if the named dependencies are
// not yet locally visible.
func (s *State) Read(key []byte, required consistency.VersionVector, raftIndex uint64) (Snapshot, error) {
	if !s.DependenciesSatisfied(required, raftIndex) {
		return Snapshot{}, ErrUnsatisfiedDependencies
	}
	return Snapshot{
		Values:         consistency.CloneRecords(s.Registers[string(key)]),
		Frontier:       s.Frontier.Clone(),
		AppliedRaftIdx: s.AppliedRaftIdx,
	}, nil
}

// Clone deep-copies the state for checkpoint/rollback.
func (s *State) Clone() *State {
	pending := make(map[string]consistency.Record, len(s.Pending))
	for key, record := range s.Pending {
		pending[key] = record.Clone()
	}
	return &State{ID: s.ID, Frontier: s.Frontier.Clone(), AppliedRaftIdx: s.AppliedRaftIdx, Registers: s.Registers.Clone(), Pending: pending}
}

// Validate rejects an incomplete or inconsistent persisted snapshot.
func (s *State) Validate() error {
	if s.ID == "" {
		return fmt.Errorf("causal snapshot has no replica ID")
	}
	if s.Frontier == nil || s.Registers == nil || s.Pending == nil {
		return fmt.Errorf("causal snapshot is incomplete")
	}
	for key, records := range s.Registers {
		for _, record := range records {
			if err := consistency.ValidateRecord(record); err != nil {
				return fmt.Errorf("invalid persisted causal register %q: %w", key, err)
			}
			if key != string(record.Key) {
				return fmt.Errorf("persisted causal register key %q does not match record key", key)
			}
		}
	}
	for key, record := range s.Pending {
		id, err := record.ID()
		if err != nil || id != key {
			return fmt.Errorf("invalid persisted causal pending record %q", key)
		}
	}
	return nil
}
