// Package application is the append-only, hash-chained audit log service.
package application

import (
	"fmt"
	"sync"
	"time"

	"github.com/nickemma/meridian/internal/audit/domain"
	"github.com/nickemma/meridian/internal/audit/ports"
)

// Log serialises all writes; the hash chain is maintained on every append.
type Log struct {
	mu       sync.RWMutex
	records  ports.Repository
	lastHash string // hash of the most recently appended record
}

// NewLog creates an empty audit log over records. The chain starts with the
// genesis hash.
func NewLog(records ports.Repository) *Log {
	return &Log{records: records, lastHash: domain.GenesisHash()}
}

// Append adds a sealed record and returns it.
func (l *Log) Append(event domain.EventType, actor, resource, outcome string, metadata map[string]string, traceID string) *domain.Record {
	l.mu.Lock()
	defer l.mu.Unlock()
	record := domain.Seal(&domain.Record{
		Event:     event,
		Actor:     actor,
		Resource:  resource,
		Outcome:   outcome,
		Metadata:  metadata,
		Timestamp: time.Now().UTC(),
		TraceID:   traceID,
	}, uint64(l.records.Len()), l.lastHash)
	l.lastHash = record.Hash
	l.records.Append(record)
	return record
}

// Verify walks the entire chain. It returns nil if the chain is intact.
func (l *Log) Verify() error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return domain.VerifyChain(l.records.From(0))
}

// Get returns the record at a given sequence number.
func (l *Log) Get(sequence uint64) (*domain.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	record, ok := l.records.At(sequence)
	if !ok {
		return nil, fmt.Errorf("sequence %d out of range", sequence)
	}
	return record, nil
}

// Since returns all records with sequence >= from.
func (l *Log) Since(from uint64) []*domain.Record {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.records.From(from)
}

// Len returns the number of records in the log.
func (l *Log) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.records.Len()
}

// LastHash returns the hash of the most recent record, for external
// verifiers to checkpoint the chain.
func (l *Log) LastHash() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastHash
}
