package adapters

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/nickemma/meridian/internal/raft/application"
	"github.com/nickemma/meridian/internal/raft/domain"
)

type recordingStateMachine struct{ applied chan uint64 }

func (m recordingStateMachine) Apply(index uint64, _ []byte) error {
	m.applied <- index
	return nil
}

func TestDurableNodeRecoversAndAppliesCommittedEntry(t *testing.T) {
	dataDir := t.TempDir()
	store, err := NewFileStateStore(filepath.Join(dataDir, "raft"))
	if err != nil {
		t.Fatal(err)
	}
	state, err := application.NewStateFromStore("node-1", store)
	if err != nil {
		t.Fatal(err)
	}
	state.BecomeCandidate()
	state.AppendEntry(state.CurrentTerm(), []byte("committed"))
	state.SetCommitIndex(1)
	if err := state.DurabilityError(); err != nil {
		t.Fatalf("persist committed entry: %v", err)
	}

	// A long election timeout keeps this single-node test away from elections:
	// the restarted node must apply the recovered commit on its own.
	opts := domain.Options{NodeID: "node-1", ElectionTimeoutMin: time.Minute, ElectionTimeoutMax: time.Minute, QuorumSize: 1}
	machine := recordingStateMachine{applied: make(chan uint64, 1)}
	restarted, err := OpenDurableNode(opts, dataDir, nil, nil, machine)
	if err != nil {
		t.Fatalf("restart durable node: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); restarted.Run(ctx) }()
	defer func() { cancel(); <-done }()

	select {
	case index := <-machine.applied:
		if index != 1 {
			t.Fatalf("applied index = %d, want 1", index)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("recovered committed entry was not applied")
	}
	deadline := time.Now().Add(5 * time.Second)
	for restarted.Status().LastApplied != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("last applied = %d, want 1", restarted.Status().LastApplied)
		}
		time.Sleep(time.Millisecond)
	}
}
