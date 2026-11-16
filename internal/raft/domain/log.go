package domain

import "bytes"

// LogEntry is a single entry in the Raft log.
// Every state change in Meridian is a LogEntry committed through consensus.
type LogEntry struct {
	Index   uint64 // position in the log — never changes once written
	Term    uint64 // which leader term this entry was created in
	Command []byte // the operation — opaque to Raft, meaningful to the application
}

// CloneEntries deep-copies a log so callers never alias stored commands.
func CloneEntries(entries []LogEntry) []LogEntry {
	clone := make([]LogEntry, len(entries))
	for index, entry := range entries {
		clone[index] = LogEntry{Index: entry.Index, Term: entry.Term, Command: bytes.Clone(entry.Command)}
	}
	return clone
}
