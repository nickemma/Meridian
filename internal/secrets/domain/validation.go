package domain

import (
	"fmt"
	"time"
)

// ErrNotFound is returned when a secret path does not exist.
type ErrNotFound struct {
	Path string
}

func (e ErrNotFound) Error() string {
	return fmt.Sprintf("secret not found: %s", e.Path)
}

// --- Validation ---

// ValidatePath checks that a secret path is well-formed.
// Paths must be slash-separated, non-empty segments.
// e.g. "services/payments/db-password" is valid
//
//	"/leading-slash" is not
func ValidatePath(path string) error {
	if path == "" {
		return fmt.Errorf("secret path cannot be empty")
	}
	if path[0] == '/' {
		return fmt.Errorf("secret path must not start with /")
	}
	if path[len(path)-1] == '/' {
		return fmt.Errorf("secret path must not end with /")
	}
	return nil
}

// ValidateValue checks that a secret value is within size limits.
func ValidateValue(value []byte) error {
	const maxSize = 64 * 1024 // 64KB
	if len(value) == 0 {
		return fmt.Errorf("secret value cannot be empty")
	}
	if len(value) > maxSize {
		return fmt.Errorf("secret value exceeds maximum size of 64KB")
	}
	return nil
}

// --- Command builders ---
// These are the only way to create commands — they enforce validation. The
// caller supplies the idempotency key and timestamp.

// PutCommand builds a validated put command.
func PutCommand(path string, value []byte, createdBy, requestID string, at time.Time) (*Command, error) {
	if err := ValidatePath(path); err != nil {
		return nil, err
	}
	if err := ValidateValue(value); err != nil {
		return nil, err
	}
	return &Command{Type: CmdPutSecret, Path: path, Value: value, CreatedBy: createdBy, RequestID: requestID, Timestamp: at}, nil
}

// DeleteCommand builds a validated delete command.
func DeleteCommand(path, createdBy, requestID string, at time.Time) (*Command, error) {
	if err := ValidatePath(path); err != nil {
		return nil, err
	}
	return &Command{Type: CmdDeleteSecret, Path: path, CreatedBy: createdBy, RequestID: requestID, Timestamp: at}, nil
}

// RotateCommand builds a validated rotate command.
func RotateCommand(path string, newValue []byte, createdBy string, gracePeriod time.Duration, requestID string, at time.Time) (*Command, error) {
	if err := ValidatePath(path); err != nil {
		return nil, err
	}
	if err := ValidateValue(newValue); err != nil {
		return nil, err
	}
	if gracePeriod < 0 {
		return nil, fmt.Errorf("grace period cannot be negative")
	}
	return &Command{Type: CmdRotateSecret, Path: path, Value: newValue, CreatedBy: createdBy, RequestID: requestID, Timestamp: at, GracePeriod: gracePeriod}, nil
}
