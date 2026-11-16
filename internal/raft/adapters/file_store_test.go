package adapters

import (
	"testing"

	"github.com/nickemma/meridian/internal/raft/application"
	"github.com/nickemma/meridian/internal/raft/domain"
)

func TestPersistentStateSurvivesRestart(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("create state store: %v", err)
	}
	state, err := application.NewStateFromStore("node-1", store)
	if err != nil {
		t.Fatalf("open persistent state: %v", err)
	}
	state.BecomeCandidate()
	state.AppendEntry(state.CurrentTerm(), []byte("first"))
	state.AppendEntry(state.CurrentTerm(), []byte("second"))
	state.SetCommitIndex(2)
	state.SetLastApplied(1)
	if err := state.DurabilityError(); err != nil {
		t.Fatalf("persist state: %v", err)
	}

	restarted, err := application.NewStateFromStore("node-1", store)
	if err != nil {
		t.Fatalf("restart persistent state: %v", err)
	}
	if restarted.CurrentTerm() != 1 || restarted.VotedFor() != "node-1" {
		t.Fatalf("recovered hard state = term:%d vote:%q", restarted.CurrentTerm(), restarted.VotedFor())
	}
	if restarted.LastLogIndex() != 2 || restarted.CommitIndex() != 2 || restarted.LastApplied() != 1 {
		t.Fatalf("recovered indexes = log:%d commit:%d applied:%d", restarted.LastLogIndex(), restarted.CommitIndex(), restarted.LastApplied())
	}
	entry, found := restarted.EntryAt(2)
	if !found || string(entry.Command) != "second" {
		t.Fatalf("recovered log entry = %+v, found:%t", entry, found)
	}
}

func TestPersistentStateRejectsInvalidRecoveryData(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("create state store: %v", err)
	}
	if err := store.Save(domain.PersistentState{CommitIndex: 1}); err == nil {
		t.Fatal("invalid state save succeeded")
	}
}

func TestSameLengthConflictingSuffixIsReplacedDurably(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	state, err := application.NewStateFromStore("node-1", store)
	if err != nil {
		t.Fatal(err)
	}
	state.BecomeFollower(3)
	state.AppendEntry(1, []byte("common"))
	state.AppendEntry(2, []byte("divergent"))
	state.SetCommitIndex(1)
	if err := state.MergeEntriesFromLeader(1, []domain.LogEntry{{Index: 2, Term: 3, Command: []byte("replacement")}}); err != nil {
		t.Fatal(err)
	}
	for _, reopen := range []bool{false, true} {
		if reopen {
			state, err = application.NewStateFromStore("node-1", store)
			if err != nil {
				t.Fatal(err)
			}
		}
		entry, found := state.EntryAt(2)
		if !found || entry.Term != 3 || string(entry.Command) != "replacement" {
			t.Fatalf("same-length conflict not replaced (reopen=%t): %+v", reopen, entry)
		}
	}
}
