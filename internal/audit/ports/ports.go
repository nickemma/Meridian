// Package ports declares where audit records are kept.
package ports

import "github.com/nickemma/meridian/internal/audit/domain"

// Repository is append-only storage for sealed records. Implementations are
// not required to be concurrency-safe; the application serialises access.
type Repository interface {
	Append(*domain.Record)
	// At returns the record with sequence, or false.
	At(sequence uint64) (*domain.Record, bool)
	// From returns every record with sequence >= from, in order.
	From(sequence uint64) []*domain.Record
	Len() int
}
