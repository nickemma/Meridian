// Package ports declares the infrastructure the secrets layer depends on.
package ports

// IDGenerator returns unique, unguessable identifiers for idempotency keys
// and lease IDs.
type IDGenerator interface {
	NewID() string
}
