package application

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/nickemma/meridian/internal/raft/domain"
)

type memoryKVStore struct {
	values map[string][]byte
	err    error
}

func newMemoryKVStore() *memoryKVStore {
	return &memoryKVStore{values: make(map[string][]byte)}
}

func (s *memoryKVStore) Get(key []byte) ([]byte, bool, error) {
	if s.err != nil {
		return nil, false, s.err
	}
	value, found := s.values[string(key)]
	return bytes.Clone(value), found, nil
}

func (s *memoryKVStore) Put(key, value []byte) error {
	if s.err != nil {
		return s.err
	}
	s.values[string(key)] = bytes.Clone(value)
	return nil
}

func (s *memoryKVStore) Delete(key []byte) error {
	if s.err != nil {
		return s.err
	}
	delete(s.values, string(key))
	return nil
}

func applyCommand(t *testing.T, machine *KVStateMachine, index uint64, command domain.Command) {
	t.Helper()
	encoded, err := command.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	if err := machine.Apply(index, encoded); err != nil {
		t.Fatalf("apply command: %v", err)
	}
}

func TestKVStateMachineAppliesCommandsInReplicatedOrder(t *testing.T) {
	store := newMemoryKVStore()
	machine := NewKVStateMachine(store)
	applyCommand(t, machine, 1, domain.Command{Type: domain.CommandPut, Key: []byte("key"), Value: []byte("one")})
	applyCommand(t, machine, 2, domain.Command{Type: domain.CommandCompareAndSet, Key: []byte("key"), Expected: []byte("one"), ExpectedExists: true, Value: []byte("two")})
	applyCommand(t, machine, 3, domain.Command{Type: domain.CommandCompareAndSet, Key: []byte("key"), Expected: []byte("one"), ExpectedExists: true, Value: []byte("three")})
	applyCommand(t, machine, 4, domain.Command{Type: domain.CommandDelete, Key: []byte("key")})

	if _, found, err := store.Get([]byte("key")); err != nil || found {
		t.Fatalf("key state after delete = found:%t err:%v, want absent", found, err)
	}
}

func TestKVStateMachinePropagatesStoreFailure(t *testing.T) {
	store := newMemoryKVStore()
	machine := NewKVStateMachine(store)
	store.err = fmt.Errorf("disk unavailable")
	encoded, err := (domain.Command{Type: domain.CommandPut, Key: []byte("key"), Value: []byte("value")}).MarshalBinary()
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	if err := machine.Apply(1, encoded); err == nil {
		t.Fatal("apply succeeded despite storage failure")
	}
}
