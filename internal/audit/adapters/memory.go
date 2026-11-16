// Package adapters provides audit record storage.
package adapters

import (
	"github.com/nickemma/meridian/internal/audit/domain"
	"github.com/nickemma/meridian/internal/audit/ports"
)

// MemoryRepository keeps records in process memory.
type MemoryRepository struct {
	records []*domain.Record
}

var _ ports.Repository = (*MemoryRepository)(nil)

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{records: make([]*domain.Record, 0)}
}

func (m *MemoryRepository) Append(record *domain.Record) { m.records = append(m.records, record) }

func (m *MemoryRepository) At(sequence uint64) (*domain.Record, bool) {
	if sequence >= uint64(len(m.records)) {
		return nil, false
	}
	return m.records[sequence], true
}

func (m *MemoryRepository) From(sequence uint64) []*domain.Record {
	if sequence >= uint64(len(m.records)) {
		return []*domain.Record{}
	}
	result := make([]*domain.Record, len(m.records)-int(sequence))
	copy(result, m.records[sequence:])
	return result
}

func (m *MemoryRepository) Len() int { return len(m.records) }
