package raft

import (
	"bytes"
	"fmt"
	"testing"
)

func TestCommandRoundTrip(t *testing.T) {
	input := Command{
		Type:           CommandCompareAndSet,
		Key:            []byte("key"),
		Value:          []byte("new"),
		Expected:       []byte("old"),
		ExpectedExists: true,
	}
	encoded, err := input.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	got, err := UnmarshalCommand(encoded)
	if err != nil {
		t.Fatalf("unmarshal command: %v", err)
	}
	if got.Type != input.Type || !bytes.Equal(got.Key, input.Key) || !bytes.Equal(got.Value, input.Value) || !bytes.Equal(got.Expected, input.Expected) || got.ExpectedExists != input.ExpectedExists {
		t.Fatalf("round trip = %+v, want %+v", got, input)
	}
}

func TestCommandRejectsMalformedPayload(t *testing.T) {
	for _, encoded := range [][]byte{
		nil,
		{CommandVersion},
		{CommandVersion, byte(CommandPut), 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		{2, byte(CommandPut), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
	} {
		if _, err := UnmarshalCommand(encoded); err == nil {
			t.Fatalf("UnmarshalCommand(%v) succeeded", encoded)
		}
	}
}

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

func applyCommand(t *testing.T, machine *KVStateMachine, index uint64, command Command) {
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
	applyCommand(t, machine, 1, Command{Type: CommandPut, Key: []byte("key"), Value: []byte("one")})
	applyCommand(t, machine, 2, Command{Type: CommandCompareAndSet, Key: []byte("key"), Expected: []byte("one"), ExpectedExists: true, Value: []byte("two")})
	applyCommand(t, machine, 3, Command{Type: CommandCompareAndSet, Key: []byte("key"), Expected: []byte("one"), ExpectedExists: true, Value: []byte("three")})
	applyCommand(t, machine, 4, Command{Type: CommandDelete, Key: []byte("key")})

	if _, found, err := store.Get([]byte("key")); err != nil || found {
		t.Fatalf("key state after delete = found:%t err:%v, want absent", found, err)
	}
}

func TestKVStateMachinePropagatesStoreFailure(t *testing.T) {
	store := newMemoryKVStore()
	machine := NewKVStateMachine(store)
	store.err = fmt.Errorf("disk unavailable")
	encoded, err := (Command{Type: CommandPut, Key: []byte("key"), Value: []byte("value")}).MarshalBinary()
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	if err := machine.Apply(1, encoded); err == nil {
		t.Fatal("apply succeeded despite storage failure")
	}
}
