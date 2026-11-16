// Package domain is the convergent multi-value-register core of Meridian's
// asynchronous gossip path. It never waits for a causal dependency: callers
// that require that guarantee must use the causal path instead.
package domain

import (
	"fmt"
	"sort"

	consistency "github.com/nickemma/meridian/internal/consistency/domain"
)

// Snapshot is a client-visible view of one key.
type Snapshot struct {
	Values   []consistency.Record
	Frontier consistency.VersionVector
}

// State is the replica's observed frontier and maximal register versions. Its
// JSON form is the durable snapshot format.
type State struct {
	ID        string                    `json:"id"`
	Frontier  consistency.VersionVector `json:"frontier"`
	Registers consistency.Registers     `json:"registers"`
}

func NewState(id string) (*State, error) {
	if id == "" {
		return nil, fmt.Errorf("replica ID is required")
	}
	return &State{ID: id, Frontier: make(consistency.VersionVector), Registers: make(consistency.Registers)}, nil
}

// NewLocalRecord versions a local write after every update this replica has
// observed, so it supersedes observed values while concurrent values from
// disconnected replicas survive as siblings.
func (s *State) NewLocalRecord(key, value []byte, tombstone bool, policyVersion uint64) (consistency.Record, error) {
	version, err := s.Frontier.Increment(s.ID)
	if err != nil {
		return consistency.Record{}, err
	}
	return consistency.Record{
		Key:           key,
		Value:         value,
		Tombstone:     tombstone,
		Version:       version,
		Dependencies:  consistency.VersionVector{},
		Origin:        s.ID,
		PolicyVersion: policyVersion,
	}.Clone(), nil
}

// ValidateIncoming rejects a malformed record or one carrying causal
// dependencies, which the eventual path cannot honour.
func ValidateIncoming(record consistency.Record) error {
	if err := consistency.ValidateRecord(record); err != nil {
		return err
	}
	if len(record.Dependencies) != 0 {
		return fmt.Errorf("eventual record must not carry causal dependencies")
	}
	return nil
}

// Apply merges record. MergeRegister is commutative over the set of received
// versions, so every connected replica reaches the same maximal sibling set.
func (s *State) Apply(record consistency.Record) error {
	if err := s.Registers.Apply(record); err != nil {
		return err
	}
	s.Frontier = s.Frontier.Merge(record.Version)
	return nil
}

func (s *State) Read(key []byte) Snapshot {
	return Snapshot{Values: consistency.CloneRecords(s.Registers[string(key)]), Frontier: s.Frontier.Clone()}
}

// Records returns all maximal records in a deterministic key order.
func (s *State) Records() []consistency.Record {
	keys := make([]string, 0, len(s.Registers))
	for key := range s.Registers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var records []consistency.Record
	for _, key := range keys {
		for _, record := range s.Registers[key] {
			records = append(records, record.Clone())
		}
	}
	return records
}

func (s *State) Clone() *State {
	return &State{ID: s.ID, Frontier: s.Frontier.Clone(), Registers: s.Registers.Clone()}
}

// Validate rejects a persisted snapshot that does not belong to id or holds
// malformed registers.
func (s *State) Validate(id string) error {
	if s.ID != id || s.Frontier == nil || s.Registers == nil {
		return fmt.Errorf("invalid eventual snapshot for replica %q", id)
	}
	for key, records := range s.Registers {
		for _, record := range records {
			if err := consistency.ValidateRecord(record); err != nil || key != string(record.Key) {
				return fmt.Errorf("invalid persisted eventual register %q", key)
			}
		}
	}
	return nil
}
