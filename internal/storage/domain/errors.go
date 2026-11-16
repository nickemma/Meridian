// Package domain defines the storage engine's failure vocabulary. It has no
// cgo dependency so callers can classify errors without linking Rust.
package domain

import (
	"errors"
	"fmt"
)

var ErrClosed = errors.New("meridian storage engine is closed")

// StatusError is returned when the native storage engine rejects an operation.
// It intentionally preserves the stable C ABI status so callers can separate a
// malformed request from an I/O failure.
type StatusError struct {
	Operation string
	Status    int32
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("storage %s failed with status %d", e.Operation, e.Status)
}
