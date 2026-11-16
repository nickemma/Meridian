// Package application adapts the context-aware storage Engine to the
// synchronous KVStore contract used by state machines.
package application

import (
	"context"

	"github.com/nickemma/meridian/internal/storage/ports"
)

// SyncStore exposes an Engine as a KVStore. State-machine application has no
// request context: a committed entry must be applied regardless of whichever
// client caused it, so every call uses context.Background.
type SyncStore struct {
	engine ports.Engine
}

var _ ports.KVStore = (*SyncStore)(nil)

func NewSyncStore(engine ports.Engine) *SyncStore {
	return &SyncStore{engine: engine}
}

func (s *SyncStore) Get(key []byte) ([]byte, bool, error) {
	return s.engine.Get(context.Background(), key)
}

func (s *SyncStore) Put(key, value []byte) error {
	return s.engine.Put(context.Background(), key, value)
}

func (s *SyncStore) Delete(key []byte) error {
	return s.engine.Delete(context.Background(), key)
}
