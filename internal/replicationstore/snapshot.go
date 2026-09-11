// Package replicationstore persists replication-path metadata in the same LSM
// engine used by the strong state machine. The binary-prefixed keys are outside
// the client namespace (which requires absolute UTF-8-like paths) and are never
// exposed through the KV API.
package replicationstore

import (
	"fmt"

	"github.com/nickemma/meridian/internal/raft"
)

// SnapshotStore is the small persistence contract used by causal and eventual
// replicas. A missing snapshot is represented by nil bytes and no error.
type SnapshotStore interface {
	LoadSnapshot() ([]byte, error)
	SaveSnapshot([]byte) error
}

type Store struct {
	store raft.KVStore
	key   []byte
}

func New(store raft.KVStore, mechanism string) (*Store, error) {
	if store == nil {
		return nil, fmt.Errorf("state store is required")
	}
	if mechanism == "" {
		return nil, fmt.Errorf("mechanism is required")
	}
	return &Store{store: store, key: []byte("\x00meridian/replication/" + mechanism + "/snapshot")}, nil
}

func (s *Store) LoadSnapshot() ([]byte, error) {
	value, found, err := s.store.Get(s.key)
	if err != nil {
		return nil, fmt.Errorf("read replication snapshot: %w", err)
	}
	if !found {
		return nil, nil
	}
	return value, nil
}

func (s *Store) SaveSnapshot(snapshot []byte) error {
	if len(snapshot) == 0 {
		return fmt.Errorf("empty replication snapshot")
	}
	if err := s.store.Put(s.key, snapshot); err != nil {
		return fmt.Errorf("write replication snapshot: %w", err)
	}
	return nil
}
