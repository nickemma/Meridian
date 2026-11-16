//go:build storageffi

package adapters

import (
	"fmt"
	"path/filepath"

	config "github.com/nickemma/meridian/internal/config/domain"
	raft "github.com/nickemma/meridian/internal/raft/application"
	raftports "github.com/nickemma/meridian/internal/raft/ports"
	storage "github.com/nickemma/meridian/internal/storage/adapters"
	storageapp "github.com/nickemma/meridian/internal/storage/application"
	storageports "github.com/nickemma/meridian/internal/storage/ports"
)

// newStateMachine opens the Rust LSM engine and wraps it as the strong KV
// state machine.
func newStateMachine(cfg *config.Config) (raftports.StateMachine, storageports.KVStore, func() error, error) {
	engine, err := storage.Open(filepath.Join(cfg.DataDir, "kv"))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open Rust storage engine: %w", err)
	}
	store := storageapp.NewSyncStore(engine)
	return raft.NewKVStateMachine(store), store, engine.Close, nil
}
