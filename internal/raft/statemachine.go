package raft

import (
	"bytes"
	"fmt"
)

// StateMachine receives committed entries in log-index order. Apply must be
// deterministic: every replica given the same entry sequence must reach the
// same state and result.
type StateMachine interface {
	Apply(index uint64, command []byte) error
}

// KVStore is the narrow durable interface needed by the first strong KV state
// machine. Its implementation will be the Rust storage adapter; it is kept
// separate from Raft so consensus never depends on cgo details.
type KVStore interface {
	Get(key []byte) (value []byte, found bool, err error)
	Put(key, value []byte) error
	Delete(key []byte) error
}

// KVStateMachine decodes versioned commands and applies them to one KVStore.
// Raft calls Apply serially, so compare-and-set is atomic with respect to the
// replicated command order. Direct writes to its store are forbidden.
type KVStateMachine struct {
	store KVStore
}

func NewKVStateMachine(store KVStore) *KVStateMachine {
	return &KVStateMachine{store: store}
}

func (m *KVStateMachine) Apply(_ uint64, encoded []byte) error {
	command, err := UnmarshalCommand(encoded)
	if err != nil {
		return err
	}
	switch command.Type {
	case CommandNoop:
		return nil
	case CommandPut:
		return m.store.Put(command.Key, command.Value)
	case CommandDelete:
		return m.store.Delete(command.Key)
	case CommandCompareAndSet:
		current, found, err := m.store.Get(command.Key)
		if err != nil {
			return fmt.Errorf("read compare-and-set key: %w", err)
		}
		if found != command.ExpectedExists || (found && !bytes.Equal(current, command.Expected)) {
			return nil
		}
		return m.store.Put(command.Key, command.Value)
	default:
		return fmt.Errorf("unsupported raft command type %d", command.Type)
	}
}

// NoopStateMachine preserves the current peer-only Raft behavior until the
// server is constructed with its durable storage-backed state machine.
// It is deliberately not a production KV implementation.
type NoopStateMachine struct{}

func (NoopStateMachine) Apply(_ uint64, _ []byte) error { return nil }
