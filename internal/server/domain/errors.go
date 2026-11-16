// Package domain defines the client storage API in transport-neutral terms:
// requests, results, the causal context a client carries between operations,
// and the error kinds every transport must preserve.
package domain

import "fmt"

// ErrorKind classifies a failed request. Transports map each kind to exactly
// one wire status so clients can switch on it instead of parsing messages.
type ErrorKind uint8

const (
	InvalidArgument ErrorKind = iota + 1
	FailedPrecondition
	DeadlineExceeded
	Unavailable
	Unimplemented
	Internal
)

// Error is a classified request failure. Context cancellation and deadline
// errors are returned unwrapped instead, so transports can report them as
// the caller's own timeout.
type Error struct {
	Kind    ErrorKind
	Message string
}

func (e *Error) Error() string { return e.Message }

func Errorf(kind ErrorKind, format string, args ...any) *Error {
	return &Error{Kind: kind, Message: fmt.Sprintf(format, args...)}
}
