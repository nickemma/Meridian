package domain

import "time"

const (
	DefaultLeaseTTL = 1 * time.Hour
	MinLeaseTTL     = 1 * time.Minute
	MaxLeaseTTL     = 24 * time.Hour
)

// Lease represents a time-bounded grant of access to a secret.
// Services must renew their lease before it expires or
// re-fetch the secret to get a new lease.
type Lease struct {
	ID        string // unique lease identifier
	Path      string // which secret this lease grants access to
	Version   uint64 // which version was fetched
	IssuedTo  string // service identity that holds this lease
	IssuedAt  time.Time
	ExpiresAt time.Time
}

// IsExpired returns true if this lease has passed its TTL.
func (l *Lease) IsExpired() bool {
	return time.Now().After(l.ExpiresAt)
}

// TTLRemaining returns how long until this lease expires.
// Returns zero if already expired.
func (l *Lease) TTLRemaining() time.Duration {
	remaining := time.Until(l.ExpiresAt)
	if remaining < 0 {
		return 0
	}
	return remaining
}
