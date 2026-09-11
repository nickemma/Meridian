package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// PersistentState is the Raft state that must survive a crash. Role, leader
// identity, election timers, and replication progress are deliberately absent:
// Raft rebuilds those volatile fields after restart.
type PersistentState struct {
	CurrentTerm uint64     `json:"current_term"`
	VotedFor    string     `json:"voted_for"`
	Log         []LogEntry `json:"log"`
	CommitIndex uint64     `json:"commit_index"`
	LastApplied uint64     `json:"last_applied"`
}

// StateStore atomically loads and saves the complete persistent Raft state.
// The interface keeps Raft independent of the file format and permits a later
// log/snapshot store without changing the protocol layer.
type StateStore interface {
	Load() (PersistentState, error)
	Save(PersistentState) error
}

// FileStateStore stores state in one atomically replaced JSON file. It is a
// correctness-first implementation: every state-changing operation fsyncs the
// replacement file and parent directory before returning success.
type FileStateStore struct {
	mu   sync.Mutex
	path string
	dir  string
}

func NewFileStateStore(dataDir string) (*FileStateStore, error) {
	if dataDir == "" {
		return nil, errors.New("raft data directory is required")
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create raft data directory: %w", err)
	}
	return &FileStateStore{path: filepath.Join(dataDir, "raft-state.json"), dir: dataDir}, nil
}

func (s *FileStateStore) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return PersistentState{}, nil
	}
	if err != nil {
		return PersistentState{}, fmt.Errorf("read raft state: %w", err)
	}
	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return PersistentState{}, fmt.Errorf("decode raft state: %w", err)
	}
	if err := validatePersistentState(state); err != nil {
		return PersistentState{}, err
	}
	return clonePersistentState(state), nil
}

func (s *FileStateStore) Save(state PersistentState) error {
	if err := validatePersistentState(state); err != nil {
		return err
	}
	data, err := json.Marshal(clonePersistentState(state))
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

func validatePersistentState(state PersistentState) error {
	if state.LastApplied > state.CommitIndex {
		return fmt.Errorf("raft state last applied %d exceeds commit index %d", state.LastApplied, state.CommitIndex)
	}
	if state.CommitIndex > uint64(len(state.Log)) {
		return fmt.Errorf("raft state commit index %d exceeds log length %d", state.CommitIndex, len(state.Log))
	}
	for index, entry := range state.Log {
		if entry.Index != uint64(index+1) {
			return fmt.Errorf("raft log entry %d has index %d", index, entry.Index)
		}
		if entry.Term == 0 {
			return fmt.Errorf("raft log entry %d has invalid term 0", entry.Index)
		}
	}
	return nil
}

func clonePersistentState(state PersistentState) PersistentState {
	clone := state
	clone.Log = make([]LogEntry, len(state.Log))
	for index, entry := range state.Log {
		clone.Log[index] = LogEntry{Index: entry.Index, Term: entry.Term, Command: bytes.Clone(entry.Command)}
	}
	return clone
}
