//go:build storageffi

package storage

import (
	"context"

	"github.com/nickemma/meridian/internal/raft"
)

// RaftStore adapts the context-aware native storage handle to Raft's narrow
// deterministic state-machine interface. Raft serializes Apply calls, so this
// adapter never exposes direct writes alongside replicated commands.
type RaftStore struct {
	engine *Engine
}

var _ raft.KVStore = (*RaftStore)(nil)

func NewRaftStore(engine *Engine) *RaftStore {
	return &RaftStore{engine: engine}
}

func (s *RaftStore) Get(key []byte) ([]byte, bool, error) {
	return s.engine.Get(context.Background(), key)
}

func (s *RaftStore) Put(key, value []byte) error {
	return s.engine.Put(context.Background(), key, value)
}

func (s *RaftStore) Delete(key []byte) error {
	return s.engine.Delete(context.Background(), key)
}
