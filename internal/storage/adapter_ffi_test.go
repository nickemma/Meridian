//go:build storageffi

package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/nickemma/meridian/internal/causal"
	"github.com/nickemma/meridian/internal/raft"
	"github.com/nickemma/meridian/internal/replicationstore"
)

func TestEnginePersistsDeletesAndCompactionAcrossReopen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	if err := engine.Put(ctx, []byte("updated"), []byte("old")); err != nil {
		t.Fatalf("put old value: %v", err)
	}
	if err := engine.Put(ctx, []byte("removed"), []byte("present")); err != nil {
		t.Fatalf("put deleted value: %v", err)
	}
	if err := engine.Flush(ctx); err != nil {
		t.Fatalf("first flush: %v", err)
	}
	if err := engine.Put(ctx, []byte("updated"), []byte("new")); err != nil {
		t.Fatalf("overwrite value: %v", err)
	}
	if err := engine.Delete(ctx, []byte("removed")); err != nil {
		t.Fatalf("delete value: %v", err)
	}
	if err := engine.Flush(ctx); err != nil {
		t.Fatalf("second flush: %v", err)
	}
	if err := engine.Compact(ctx); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen engine: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	value, found, err := reopened.Get(ctx, []byte("updated"))
	if err != nil {
		t.Fatalf("get overwritten value: %v", err)
	}
	if !found || string(value) != "new" {
		t.Fatalf("got (%q, %t), want (new, true)", value, found)
	}
	_, found, err = reopened.Get(ctx, []byte("removed"))
	if err != nil {
		t.Fatalf("get deleted value: %v", err)
	}
	if found {
		t.Fatal("deleted key was present after reopen")
	}
}

func TestRaftStateMachinePersistsThroughRustAdapter(t *testing.T) {
	ctx := context.Background()
	engine, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	machine := raft.NewKVStateMachine(NewRaftStore(engine))

	put, err := (raft.Command{Type: raft.CommandPut, Key: []byte("key"), Value: []byte("value")}).MarshalBinary()
	if err != nil {
		t.Fatalf("marshal put: %v", err)
	}
	if err := machine.Apply(1, put); err != nil {
		t.Fatalf("apply put: %v", err)
	}
	deleteCommand, err := (raft.Command{Type: raft.CommandDelete, Key: []byte("key")}).MarshalBinary()
	if err != nil {
		t.Fatalf("marshal delete: %v", err)
	}
	if err := machine.Apply(2, deleteCommand); err != nil {
		t.Fatalf("apply delete: %v", err)
	}

	if err := engine.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	_, found, err := engine.Get(ctx, []byte("key"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if found {
		t.Fatal("deleted Raft state-machine key was present")
	}
}

func TestEngineRejectsCancelledContextAndOperationsAfterClose(t *testing.T) {
	t.Parallel()
	engine, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := engine.Put(ctx, []byte("key"), []byte("value")); !errors.Is(err, context.Canceled) {
		t.Fatalf("put error = %v, want context cancellation", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}
	if err := engine.Delete(context.Background(), []byte("key")); !errors.Is(err, ErrClosed) {
		t.Fatalf("delete error = %v, want ErrClosed", err)
	}
}

func TestCausalSnapshotPersistsThroughRustAdapter(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	store, err := replicationstore.New(NewRaftStore(engine), "causal")
	if err != nil {
		t.Fatal(err)
	}
	replica, err := causal.OpenReplica("node-1", store)
	if err != nil {
		t.Fatal(err)
	}
	if err := replica.ObserveRaft(3); err != nil {
		t.Fatal(err)
	}
	record, err := replica.Write([]byte("/causal/key"), []byte("value"), false, nil, 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopenedStore, err := replicationstore.New(NewRaftStore(reopened), "causal")
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := causal.OpenReplica("node-1", reopenedStore)
	if err != nil {
		t.Fatal(err)
	}
	got, err := restarted.Read([]byte("/causal/key"), record.Version, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Values) != 1 || string(got.Values[0].Value) != "value" {
		t.Fatalf("restored causal values = %#v", got.Values)
	}
}
