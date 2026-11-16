// Package adapters connects the Raft core to the filesystem and to gRPC.
package adapters

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/nickemma/meridian/internal/raft/domain"
	"github.com/nickemma/meridian/internal/raft/ports"
)

// FileStateStore stores state in one atomically replaced JSON file. It is a
// correctness-first implementation: every state-changing operation fsyncs the
// replacement file and parent directory before returning success.
type FileStateStore struct {
	mu   sync.Mutex
	path string
	dir  string
}

var _ ports.StateStore = (*FileStateStore)(nil)

func NewFileStateStore(dataDir string) (*FileStateStore, error) {
	if dataDir == "" {
		return nil, errors.New("raft data directory is required")
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create raft data directory: %w", err)
	}
	return &FileStateStore{path: filepath.Join(dataDir, "raft-state.json"), dir: dataDir}, nil
}

func (s *FileStateStore) Load() (domain.PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return domain.PersistentState{}, nil
	}
	if err != nil {
		return domain.PersistentState{}, fmt.Errorf("read raft state: %w", err)
	}
	var state domain.PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return domain.PersistentState{}, fmt.Errorf("decode raft state: %w", err)
	}
	if err := state.Validate(); err != nil {
		return domain.PersistentState{}, err
	}
	return state.Clone(), nil
}

func (s *FileStateStore) Save(state domain.PersistentState) error {
	if err := state.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(state.Clone())
	if err != nil {
		return fmt.Errorf("encode raft state: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	temporary, err := os.CreateTemp(s.dir, ".raft-state-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary raft state: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return fmt.Errorf("set raft state permissions: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return fmt.Errorf("write raft state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("sync raft state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close raft state: %w", err)
	}
	if err := os.Rename(temporaryPath, s.path); err != nil {
		return fmt.Errorf("publish raft state: %w", err)
	}
	directory, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("open raft state directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync raft state directory: %w", err)
	}
	return nil
}
