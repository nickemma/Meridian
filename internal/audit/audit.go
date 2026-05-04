package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// EventType identifies what kind of event an audit record describes.
type EventType string

const (
	EventSecretRead    EventType = "secret.read"
	EventSecretWrite   EventType = "secret.write"
	EventSecretRotate  EventType = "secret.rotate"
	EventSecretDelete  EventType = "secret.delete"
	EventPolicyAllow   EventType = "policy.allow"
	EventPolicyDeny    EventType = "policy.deny"
	EventLeaderElected EventType = "raft.leader_elected"
	EventNodeJoined    EventType = "raft.node_joined"
)

// Record is a single immutable audit event.
// Once written it is never modified.
type Record struct {
	// Sequence is the position of this record in the audit log.
	// Monotonically increasing, never reused.
	Sequence uint64 `json:"sequence"`

	// Event identifies what happened.
	Event EventType `json:"event"`

	// Actor is the service identity that caused the event.
	Actor string `json:"actor"`

	// Resource is what was acted on — typically a secret path.
	Resource string `json:"resource"`

	// Outcome describes the result — "success", "denied", "error".
	Outcome string `json:"outcome"`

	// Metadata holds event-specific details.
	// e.g. {"version": "3", "policy": "payments-policy"}
	Metadata map[string]string `json:"metadata,omitempty"`

	// Timestamp is when the event occurred.
	Timestamp time.Time `json:"timestamp"`

	// TraceID links this event to a distributed trace.
	TraceID string `json:"trace_id,omitempty"`

	// PrevHash is the SHA-256 hash of the previous record's Hash field.
	// For the first record this is the hash of an empty string.
	// This is what makes the chain tamper-evident.
	PrevHash string `json:"prev_hash"`

	// Hash is the SHA-256 hash of this record's content
	// (all fields except Hash itself).
	Hash string `json:"hash"`
}

// Log is an append-only, hash-chained audit log.
// All writes are serialised through a mutex.
// The hash chain is maintained automatically on every append.
type Log struct {
	mu       sync.RWMutex
	records  []*Record
	lastHash string // hash of the most recently appended record
}

// NewLog creates an empty audit log.
// The chain starts with the hash of an empty string.
func NewLog() *Log {
	return &Log{
		records:  make([]*Record, 0),
		lastHash: hashOf(""), // genesis hash
	}
}

// Append adds a new record to the audit log.
// The hash chain is updated automatically.
// Returns the completed record with its hash fields populated.
func (l *Log) Append(
	event EventType,
	actor string,
	resource string,
	outcome string,
	metadata map[string]string,
	traceID string,
) *Record {
	l.mu.Lock()
	defer l.mu.Unlock()

	record := &Record{
		Sequence:  uint64(len(l.records)),
		Event:     event,
		Actor:     actor,
		Resource:  resource,
		Outcome:   outcome,
		Metadata:  metadata,
		Timestamp: time.Now().UTC(),
		TraceID:   traceID,
		PrevHash:  l.lastHash,
	}

	// Compute this record's hash over all fields except Hash itself.
	record.Hash = computeHash(record)
	l.lastHash = record.Hash

	l.records = append(l.records, record)
	return record
}

// Verify walks the entire chain and checks that every record's
// hash is consistent with its content and the previous record's hash.
// Returns nil if the chain is intact, an error describing the
// first corruption found if not.
func (l *Log) Verify() error {
	l.mu.RLock()
	defer l.mu.RUnlock()

	expectedPrevHash := hashOf("")

	for _, record := range l.records {
		// Check the prev_hash links correctly.
		if record.PrevHash != expectedPrevHash {
			return fmt.Errorf(
				"chain broken at sequence %d: "+
					"expected prev_hash %q got %q",
				record.Sequence, expectedPrevHash, record.PrevHash,
			)
		}

		// Recompute the hash and check it matches.
		expected := computeHash(record)
		if record.Hash != expected {
			return fmt.Errorf(
				"record %d has been tampered with: "+
					"stored hash %q does not match computed %q",
				record.Sequence, record.Hash, expected,
			)
		}

		expectedPrevHash = record.Hash
	}

	return nil
}

// Get returns the record at a given sequence number.
func (l *Log) Get(sequence uint64) (*Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if sequence >= uint64(len(l.records)) {
		return nil, fmt.Errorf("sequence %d out of range", sequence)
	}
	return l.records[sequence], nil
}

// Since returns all records with sequence >= from.
// Used for audit log queries.
func (l *Log) Since(from uint64) []*Record {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if from >= uint64(len(l.records)) {
		return []*Record{}
	}
	result := make([]*Record, len(l.records)-int(from))
	copy(result, l.records[from:])
	return result
}

// Len returns the number of records in the log.
func (l *Log) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.records)
}

// LastHash returns the hash of the most recent record.
// Used by external verifiers to checkpoint the chain.
func (l *Log) LastHash() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastHash
}

// computeHash produces a SHA-256 hash of a record's content.
// The Hash field itself is excluded — we are hashing the content,
// not the hash of the content.
func computeHash(r *Record) string {
	// Serialize all fields except Hash into a canonical form.
	content := struct {
		Sequence  uint64            `json:"sequence"`
		Event     EventType         `json:"event"`
		Actor     string            `json:"actor"`
		Resource  string            `json:"resource"`
		Outcome   string            `json:"outcome"`
		Metadata  map[string]string `json:"metadata,omitempty"`
		Timestamp time.Time         `json:"timestamp"`
		TraceID   string            `json:"trace_id,omitempty"`
		PrevHash  string            `json:"prev_hash"`
	}{
		Sequence:  r.Sequence,
		Event:     r.Event,
		Actor:     r.Actor,
		Resource:  r.Resource,
		Outcome:   r.Outcome,
		Metadata:  r.Metadata,
		Timestamp: r.Timestamp,
		TraceID:   r.TraceID,
		PrevHash:  r.PrevHash,
	}

	data, _ := json.Marshal(content)
	return hashOf(string(data))
}

func hashOf(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}
