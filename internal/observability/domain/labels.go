// Package domain defines the bounded label vocabulary for storage telemetry.
// Labels never carry keys, values, tenants or credentials.
package domain

import consistency "github.com/nickemma/meridian/internal/consistency/domain"

const (
	// ClassNone labels requests that carry no consistency class (status).
	ClassNone = "none"
	// ClassInvalid labels a request whose class is unspecified or unknown.
	ClassInvalid = "invalid"

	OutcomeOK    = "ok"
	OutcomeError = "error"
)

// ClassLabel returns the label for a request's consistency class.
func ClassLabel(class consistency.Class) string {
	if class.Strength() == 0 {
		return ClassInvalid
	}
	return class.String()
}

// Outcome reduces an RPC error to the replication outcome label.
func Outcome(err error) string {
	if err != nil {
		return OutcomeError
	}
	return OutcomeOK
}
