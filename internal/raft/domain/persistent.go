package domain

import "fmt"

// PersistentState is the Raft state that must survive a crash. Role, leader
// identity, election timers, and replication progress are deliberately absent:
// Raft rebuilds those volatile fields after restart.
type PersistentState struct {
	CurrentTerm uint64     `json:"current_term"`
	VotedFor    string     `json:"voted_for"`
	Log         []LogEntry `json:"log"`
	CommitIndex uint64     `json:"commit_index"`
	LastApplied uint64     `json:"last_applied"`
}

// Validate rejects recovery data that would let a node participate with an
// invented or truncated history.
func (s PersistentState) Validate() error {
	if s.LastApplied > s.CommitIndex {
		return fmt.Errorf("raft state last applied %d exceeds commit index %d", s.LastApplied, s.CommitIndex)
	}
	if s.CommitIndex > uint64(len(s.Log)) {
		return fmt.Errorf("raft state commit index %d exceeds log length %d", s.CommitIndex, len(s.Log))
	}
	for index, entry := range s.Log {
		if entry.Index != uint64(index+1) {
			return fmt.Errorf("raft log entry %d has index %d", index, entry.Index)
		}
		if entry.Term == 0 {
			return fmt.Errorf("raft log entry %d has invalid term 0", entry.Index)
		}
	}
	return nil
}

// Clone deep-copies the log.
func (s PersistentState) Clone() PersistentState {
	clone := s
	clone.Log = CloneEntries(s.Log)
	return clone
}
