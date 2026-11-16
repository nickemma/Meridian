// Package domain holds Raft's protocol vocabulary: roles, log entries,
// replicated commands, durable state, RPC messages and the pure safety rules.
// It has no transport, storage or metrics dependency.
package domain

// Role represents the current state of a Raft node.
// A node is always in exactly one of these three states.
type Role int

const (
	Follower  Role = iota // default state - listens, votes, never initiates
	Candidate             // running for leader
	Leader                // in-charge - replicates, log, sends heartbeats
)

// String returns the protocol role for status and structured logs.
func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}
