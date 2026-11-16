// Package adapters provides policy storage.
package adapters

import (
	"github.com/nickemma/meridian/internal/policy/domain"
	"github.com/nickemma/meridian/internal/policy/ports"
)

// MemoryRepository keeps policies in process memory.
type MemoryRepository struct {
	policies map[string]*domain.Policy // policy name → policy
}

var _ ports.Repository = (*MemoryRepository)(nil)

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{policies: make(map[string]*domain.Policy)}
}

func (m *MemoryRepository) Put(policy *domain.Policy) { m.policies[policy.Name] = policy }
func (m *MemoryRepository) Delete(name string)        { delete(m.policies, name) }

func (m *MemoryRepository) All() []*domain.Policy {
	all := make([]*domain.Policy, 0, len(m.policies))
	for _, policy := range m.policies {
		all = append(all, policy)
	}
	return all
}
