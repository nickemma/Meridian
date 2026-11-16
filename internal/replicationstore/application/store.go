// Package application persists a replica's snapshot in the same LSM engine
// used by the strong state machine.
package application

import (
	"fmt"

	"github.com/nickemma/meridian/internal/replicationstore/domain"
	"github.com/nickemma/meridian/internal/replicationstore/ports"
)

type Store struct {
	store ports.KVStore
	key   []byte
}

var _ ports.SnapshotStore = (*Store)(nil)

func New(store ports.KVStore, mechanism string) (*Store, error) {
	if store == nil {
		return nil, fmt.Errorf("state store is required")
	}
	key, err := domain.SnapshotKey(mechanism)
	if err != nil {
		return nil, err
	}
	return &Store{store: store, key: key}, nil
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
