package application

import (
	"bytes"
	"testing"
)

type memoryKV map[string][]byte

func (m memoryKV) Get(key []byte) ([]byte, bool, error) {
	value, found := m[string(key)]
	return value, found, nil
}
func (m memoryKV) Put(key, value []byte) error { m[string(key)] = bytes.Clone(value); return nil }
func (m memoryKV) Delete(key []byte) error     { delete(m, string(key)); return nil }

func TestStoreRoundTripsAndIsolatesMechanisms(t *testing.T) {
	kv := memoryKV{}
	causal, err := New(kv, "causal")
	if err != nil {
		t.Fatal(err)
	}
	eventual, err := New(kv, "eventual")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot, err := causal.LoadSnapshot(); err != nil || snapshot != nil {
		t.Fatalf("missing snapshot = (%q, %v), want (nil, nil)", snapshot, err)
	}
	if err := causal.SaveSnapshot([]byte("c")); err != nil {
		t.Fatal(err)
	}
	if snapshot, _ := eventual.LoadSnapshot(); snapshot != nil {
		t.Fatal("eventual snapshot aliased the causal key")
	}
	if snapshot, _ := causal.LoadSnapshot(); string(snapshot) != "c" {
		t.Fatalf("snapshot = %q", snapshot)
	}
	if err := causal.SaveSnapshot(nil); err == nil {
		t.Fatal("empty snapshot accepted")
	}
	if _, err := New(kv, ""); err == nil {
		t.Fatal("empty mechanism accepted")
	}
}
