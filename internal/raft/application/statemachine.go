package application

import (
	"bytes"
	"fmt"

	"github.com/nickemma/meridian/internal/raft/domain"
	"github.com/nickemma/meridian/internal/raft/ports"
)

// KVStateMachine decodes versioned commands and applies them to one KVStore.
// Raft calls Apply serially, so compare-and-set is atomic with respect to the
// replicated command order. Direct writes to its store are forbidden.
type KVStateMachine struct {
	store ports.KVStore
}

func NewKVStateMachine(store ports.KVStore) *KVStateMachine {
	return &KVStateMachine{store: store}
}

func (m *KVStateMachine) Apply(_ uint64, encoded []byte) error {
	command, err := domain.UnmarshalCommand(encoded)
	if err != nil {
		return err
	}
	switch command.Type {
	case domain.CommandNoop:
		return nil
	case domain.CommandPut:
		return m.store.Put(command.Key, command.Value)
	case domain.CommandDelete:
		return m.store.Delete(command.Key)
	case domain.CommandCompareAndSet:
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

// NoopStateMachine applies nothing. It backs protocol tests and nodes
// constructed without a durable storage-backed state machine.
// It is deliberately not a production KV implementation.
type NoopStateMachine struct{}

var (
	_ ports.StateMachine = NoopStateMachine{}
	_ ports.StateMachine = (*KVStateMachine)(nil)
)

func (NoopStateMachine) Apply(_ uint64, _ []byte) error { return nil }
