// Package application implements the namespace-policy registry: the single
// longest-prefix admission point for every consistency class.
package application

import (
	"fmt"
	"strings"
	"sync"

	"github.com/nickemma/meridian/internal/consistency/domain"
	"github.com/nickemma/meridian/internal/consistency/ports"
)

// PolicyRegistry holds immutable namespace policies.
type PolicyRegistry struct {
	mu       sync.RWMutex
	policies map[string]domain.NamespacePolicy
}

var _ ports.PolicyResolver = (*PolicyRegistry)(nil)

func NewPolicyRegistry() *PolicyRegistry {
	return &PolicyRegistry{policies: make(map[string]domain.NamespacePolicy)}
}

// NewConfiguredRegistry installs configured, or the development default when
// none are configured.
func NewConfiguredRegistry(configured []domain.NamespacePolicy) (*PolicyRegistry, error) {
	registry := NewPolicyRegistry()
	if len(configured) == 0 {
		configured = domain.DefaultPolicies()
	}
	for _, policy := range configured {
		if err := registry.Install(policy); err != nil {
			return nil, fmt.Errorf("install namespace policy %q: %w", policy.Prefix, err)
		}
	}
	return registry, nil
}

func (r *PolicyRegistry) Install(policy domain.NamespacePolicy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.policies[policy.Prefix]; exists {
		return fmt.Errorf("%w: %s", domain.ErrPolicyExists, policy.Prefix)
	}
	r.policies[policy.Prefix] = policy
	return nil
}

func (r *PolicyRegistry) Lookup(key string) (domain.NamespacePolicy, error) {
	if !strings.HasPrefix(key, "/") {
		return domain.NamespacePolicy{}, fmt.Errorf("key must be an absolute namespace path")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var selected domain.NamespacePolicy
	found := false
	for prefix, policy := range r.policies {
		if strings.HasPrefix(key, prefix) && (!found || len(prefix) > len(selected.Prefix)) {
			selected = policy
			found = true
		}
	}
	if !found {
		return domain.NamespacePolicy{}, domain.ErrNoPolicy
	}
	return selected, nil
}

func (r *PolicyRegistry) ValidateWrite(key string, requested domain.Class, expectedVersion uint64) (domain.NamespacePolicy, error) {
	policy, err := r.Lookup(key)
	if err != nil {
		return domain.NamespacePolicy{}, err
	}
	if err := policy.AdmitWrite(key, requested, expectedVersion); err != nil {
		return domain.NamespacePolicy{}, err
	}
	return policy, nil
}

func (r *PolicyRegistry) ValidateRead(key string, requested domain.Class) (domain.NamespacePolicy, error) {
	policy, err := r.Lookup(key)
	if err != nil {
		return domain.NamespacePolicy{}, err
	}
	if err := policy.AdmitRead(key, requested); err != nil {
		return domain.NamespacePolicy{}, err
	}
	return policy, nil
}
