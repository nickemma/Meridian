//go:build storageffi

package server

import (
	"fmt"
	"path/filepath"

	"github.com/nickemma/meridian/internal/config"
	"github.com/nickemma/meridian/internal/raft"
	"github.com/nickemma/meridian/internal/storage"
)

func newStateMachine(cfg *config.Config) (raft.StateMachine, raft.KVStore, func() error, error) {
	engine, err := storage.Open(filepath.Join(cfg.DataDir, "kv"))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open Rust storage engine: %w", err)
	}
	store := storage.NewRaftStore(engine)
	return raft.NewKVStateMachine(store), store, engine.Close, nil
}
