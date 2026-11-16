package domain

import (
	"time"

	consistency "github.com/nickemma/meridian/internal/consistency/domain"
)

// CausalContext is the opaque token a client returns with its next request.
// VectorClock is the stable binary version-vector encoding.
type CausalContext struct {
	VectorClock   []byte
	RaftIndex     uint64
	PolicyVersion uint64
}

// Request is one KV operation. Class zero means unspecified or unknown.
type Request struct {
	RequestID        string
	Key              []byte
	Value            []byte
	Class            consistency.Class
	Context          *CausalContext
	DeadlineUnixNano int64
	// CompareAndSet only: ExpectedExists distinguishes absent from empty.
	ExpectedExists bool
	Expected       []byte
}

// PolicyVersion is the namespace policy version the client wrote against.
func (r Request) PolicyVersion() uint64 {
	if r.Context == nil {
		return 0
	}
	return r.Context.PolicyVersion
}

// Deadline reports the client's absolute deadline, if any.
func (r Request) Deadline() (time.Time, bool) {
	if r.DeadlineUnixNano <= 0 {
		return time.Time{}, false
	}
	return time.Unix(0, r.DeadlineUnixNano), true
}

// Dependencies is a decoded CausalContext.
type Dependencies struct {
	Vector        consistency.VersionVector
	RaftIndex     uint64
	PolicyVersion uint64
}

// Version is a result context: what a client must pass back to observe this
// result's effects. The transport encodes Vector.
type Version struct {
	Vector        consistency.VersionVector
	RaftIndex     uint64
	PolicyVersion uint64
}

// VersionedValue is one visible sibling of a multi-value register.
type VersionedValue struct {
	Value     []byte
	Tombstone bool
	Context   Version
}

type ReadResult struct {
	Found     bool
	Value     []byte
	Served    consistency.Class
	RaftIndex uint64
	// Context and Versions are set only by causal and eventual reads.
	Context  *Version
	Versions []VersionedValue
}

type WriteResult struct {
	RaftIndex uint64
	Served    consistency.Class
	Context   *Version
}

type CompareAndSetResult struct {
	Swapped       bool
	CurrentExists bool
	CurrentValue  []byte
	RaftIndex     uint64
	Served        consistency.Class
}
