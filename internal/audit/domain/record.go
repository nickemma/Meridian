// Package domain defines immutable, hash-chained audit records and the
// chain-verification rule.
package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

// ComputeHash produces a SHA-256 hash of a record's content.
// The Hash field itself is excluded — we are hashing the content,
// not the hash of the content.
func ComputeHash(r *Record) string {
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

// GenesisHash is the PrevHash of the first record: the hash of "".
func GenesisHash() string { return hashOf("") }

// Seal fills in a new record's chain fields and returns it.
func Seal(record *Record, sequence uint64, prevHash string) *Record {
	record.Sequence = sequence
	record.PrevHash = prevHash
	record.Hash = ComputeHash(record)
	return record
}

// VerifyChain checks that every record's hash matches its content and links
// to the previous record. It returns the first corruption found.
func VerifyChain(records []*Record) error {
	expectedPrevHash := GenesisHash()
	for _, record := range records {
		if record.PrevHash != expectedPrevHash {
			return fmt.Errorf(
				"chain broken at sequence %d: "+
					"expected prev_hash %q got %q",
				record.Sequence, expectedPrevHash, record.PrevHash,
			)
		}
		if expected := ComputeHash(record); record.Hash != expected {
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
