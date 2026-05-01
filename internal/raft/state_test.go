package raft

import (
	"testing"
)

func TestNewState_InitialValues(t *testing.T) {
	s := NewState("node-1")

	if s.CurrentTerm() != 0 {
		t.Errorf("expected term 0, got %d", s.CurrentTerm())
	}
	if s.GetRole() != Follower {
		// t.Errorf("expected Follower, got %s", s.GetRole())
		t.Errorf("expected Leader, got %s", s.GetRole().string())
	}
	if s.VotedFor() != "" {
		t.Errorf("expected no vote, got %s", s.VotedFor())
	}
	if s.LastLogIndex() != 0 {
		t.Errorf("expected empty log, got index %d", s.LastLogIndex())
	}
}

func TestBecomeCandidate_IncrementsTermAndVotesSelf(t *testing.T) {
	s := NewState("node-1")
	term := s.BecomeCandidate()

	if term != 1 {
		t.Errorf("expected term 1, got %d", term)
	}
	if s.GetRole() != Candidate {
		t.Errorf("expected Leader, got %s", s.GetRole().string())
	}
	if s.VotedFor() != "node-1" {
		t.Errorf("expected voted for self, got %s", s.VotedFor())
	}
}

func TestBecomeLeader_InitialisesReplicationState(t *testing.T) {
	s := NewState("node-1")
	s.BecomeCandidate()

	// Add an entry so lastLogIndex is non-zero
	s.AppendEntry(1, []byte("cmd"))
	s.BecomeLeader([]string{"node-2", "node-3"})

	if s.GetRole() != Leader {
		t.Errorf("expected Leader, got %s", s.GetRole().string())
	}
	// nextIndex for each peer should be lastLogIndex + 1 = 2
	if s.GetNextIndex("node-2") != 2 {
		t.Errorf("expected nextIndex 2 for node-2, got %d", s.GetNextIndex("node-2"))
	}
	if s.GetMatchIndex("node-3") != 0 {
		t.Errorf("expected matchIndex 0 for node-3, got %d", s.GetMatchIndex("node-3"))
	}
}

func TestBecomeFollower_ResetsState(t *testing.T) {
	s := NewState("node-1")
	s.BecomeCandidate()
	s.BecomeFollower(5)

	if s.CurrentTerm() != 5 {
		t.Errorf("expected term 5, got %d", s.CurrentTerm())
	}
	if s.GetRole() != Follower {
		t.Errorf("expected Leader, got %s", s.GetRole().string())
	}
	if s.VotedFor() != "" {
		t.Errorf("expected cleared vote, got %s", s.VotedFor())
	}
}

func TestLog_AppendAndRetrieve(t *testing.T) {
	s := NewState("node-1")

	s.AppendEntry(1, []byte("first"))
	s.AppendEntry(1, []byte("second"))

	if s.LastLogIndex() != 2 {
		t.Errorf("expected last index 2, got %d", s.LastLogIndex())
	}

	entry, ok := s.EntryAt(1)
	if !ok {
		t.Fatal("expected entry at index 1")
	}
	if string(entry.Command) != "first" {
		t.Errorf("expected 'first', got %s", entry.Command)
	}
}

func TestLog_TruncateFrom(t *testing.T) {
	s := NewState("node-1")

	s.AppendEntry(1, []byte("a"))
	s.AppendEntry(1, []byte("b"))
	s.AppendEntry(2, []byte("c"))

	// Truncate from index 2 — removes b and c
	s.TruncateFrom(2)

	if s.LastLogIndex() != 1 {
		t.Errorf("expected last index 1 after truncation, got %d", s.LastLogIndex())
	}
}

func TestCommitIndex_NeverMovesBackward(t *testing.T) {
	s := NewState("node-1")

	s.SetCommitIndex(5)
	s.SetCommitIndex(3) // attempt to move backward

	if s.CommitIndex() != 5 {
		t.Errorf("expected commit index 5, got %d", s.CommitIndex())
	}
}
