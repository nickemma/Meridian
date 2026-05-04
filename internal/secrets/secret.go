package secrets

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"time"
)

// Secret represents a secret with a single versioned stored in Meridian.
type Secret struct {
	Path      string // e.g. "services/payments/db-password"
	Value     []byte // the secret bytes — encrypted at rest in Phase 5
	Version   uint64 // monotonically increasing — starts at 1
	CreatedAt time.Time
	UpdatedAt time.Time
	CreatedBy string    // service identity that wrote this secret
	ExpiresAt time.Time // zero means no expiry
}

// SecretVersion is a single historical version of a secret.
// All versions are kept until explicitly purged.
type SecretVersion struct {
	Version   uint64
	Value     []byte
	CreatedAt time.Time
	CreatedBy string
	Revoked   bool // true if this version has been explicitly revoked
}

// CommandType identifies what operation a Raft command represents.
type CommandType uint8

const (
	CmdPutSecret    CommandType = 0x01
	CmdDeleteSecret CommandType = 0x02
	CmdRotateSecret CommandType = 0x03
)

// Command is the structure serialized into every Raft log entry
// by the secrets layer. Self-contained — every node can apply it
// without any external context.
type Command struct {
	Type      CommandType `json:"type"`
	Path      string      `json:"path"`
	Value     []byte      `json:"value,omitempty"`
	CreatedBy string      `json:"created_by"`
	RequestID string      `json:"request_id"` // idempotency key
	Timestamp time.Time   `json:"timestamp"`
}

// Encode serializes a Command into bytes for the Raft log.
func (c *Command) Encode() ([]byte, error) {
	return json.Marshal(c)
}

// DecodeCommand deserializes bytes from the Raft log into a Command.
func DecodeCommand(data []byte) (*Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to decode secret command: %w", err)
	}
	return &cmd, nil
}

// newRequestID generates a unique idempotency key for each command.
func newRequestID() string {
	b := make([]byte, 16) // 128 bits
	_, err := rand.Read(b)
	if err != nil {
		panic(fmt.Sprintf("failed to generate request ID: %v", err))
	}
	return fmt.Sprintf("%x", b)
}
