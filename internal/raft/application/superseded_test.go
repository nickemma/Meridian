package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nickemma/meridian/internal/raft/domain"
)

// A deposed leader's pending command can be overwritten by the new leader's
// different entry at the same index. Applying that index must not report the
// deposed leader's command as committed: doing so acknowledges a write that
// was discarded.
func TestSubmitAndWaitFailsWhenEntryIsSupersededAtSameIndex(t *testing.T) {
	n := newLeaderNode("node-1", []string{"node-2", "node-3"}) // leader in term 2
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := n.SubmitAndWait(ctx, []byte("deposed leader's write"))
		result <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for n.state.LastLogIndex() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("command was not appended")
		}
		time.Sleep(time.Millisecond)
	}

	// The new term-3 leader committed a different command at index 1.
	response := n.handleAppendEntries(domain.AppendEntriesRequest{
		Term: 3, LeaderID: "node-2", LeaderCommit: 1,
		Entries: []domain.LogEntry{{Index: 1, Term: 3, Command: []byte("new leader's write")}},
	})
	if !response.Success {
		t.Fatal("new leader's entry rejected")
	}
	n.applyCommitted()

	select {
	case err := <-result:
		if !errors.Is(err, domain.ErrCommandSuperseded) {
			t.Fatalf("SubmitAndWait = %v, want ErrCommandSuperseded", err)
		}
	case <-ctx.Done():
		t.Fatal("SubmitAndWait did not return")
	}
}
