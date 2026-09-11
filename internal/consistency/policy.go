package consistency

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

type Class uint8

const (
	Strong Class = iota + 1
	Causal
	Eventual
)

func (c Class) String() string {
	switch c {
	case Strong:
		return "strong"
	case Causal:
		return "causal"
	case Eventual:
		return "eventual"
	default:
		return "unknown"
	}
}

// NamespacePolicy assigns exactly one write path to a path prefix. Policies
// are immutable after installation; production installation will be a strong
// Raft command so every node uses the same longest-prefix result.
type NamespacePolicy struct {
	Prefix  string
	Class   Class
	Version uint64
}

var (
	ErrNoPolicy      = errors.New("no namespace policy matches key")
	ErrPolicyExists  = errors.New("namespace policy is immutable")
	ErrWriteClass    = errors.New("write consistency does not match namespace policy")
	ErrReadTooStrong = errors.New("requested read consistency exceeds namespace policy")
)

type PolicyRegistry struct {
	mu       sync.RWMutex
	policies map[string]NamespacePolicy
}

func NewPolicyRegistry() *PolicyRegistry {
	return &PolicyRegistry{policies: make(map[string]NamespacePolicy)}
}

func (r *PolicyRegistry) Install(policy NamespacePolicy) error {
	if err := validatePolicy(policy); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.policies[policy.Prefix]; exists {
		return fmt.Errorf("%w: %s", ErrPolicyExists, policy.Prefix)
	}
	r.policies[policy.Prefix] = policy
	return nil
}

func (r *PolicyRegistry) Lookup(key string) (NamespacePolicy, error) {
	if !strings.HasPrefix(key, "/") {
		return NamespacePolicy{}, fmt.Errorf("key must be an absolute namespace path")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var selected NamespacePolicy
	found := false
	for prefix, policy := range r.policies {
		if strings.HasPrefix(key, prefix) && (!found || len(prefix) > len(selected.Prefix)) {
			selected = policy
			found = true
		}
	}
	if !found {
		return NamespacePolicy{}, ErrNoPolicy
	}
	return selected, nil
}

func (r *PolicyRegistry) ValidateWrite(key string, requested Class, expectedVersion uint64) (NamespacePolicy, error) {
	policy, err := r.Lookup(key)
	if err != nil {
		return NamespacePolicy{}, err
	}
	if policy.Version != expectedVersion {
		return NamespacePolicy{}, fmt.Errorf("policy version %d does not match current version %d", expectedVersion, policy.Version)
	}
	if requested != policy.Class {
		return NamespacePolicy{}, fmt.Errorf("%w: key %s is %s", ErrWriteClass, key, policy.Class)
	}
	return policy, nil
}

func (r *PolicyRegistry) ValidateRead(key string, requested Class) (NamespacePolicy, error) {
	policy, err := r.Lookup(key)
	if err != nil {
		return NamespacePolicy{}, err
	}
	if strength(requested) > strength(policy.Class) {
		return NamespacePolicy{}, fmt.Errorf("%w: key %s is %s", ErrReadTooStrong, key, policy.Class)
	}
	return policy, nil
}

func validatePolicy(policy NamespacePolicy) error {
	if policy.Prefix == "/" {
		// Root is a valid default policy.
	} else if !strings.HasPrefix(policy.Prefix, "/") || !strings.HasSuffix(policy.Prefix, "/") {
		return fmt.Errorf("namespace prefix must be / or an absolute path ending in /")
	}
	if policy.Version == 0 {
		return fmt.Errorf("namespace policy version is required")
	}
	if strength(policy.Class) == 0 {
		return fmt.Errorf("namespace policy has invalid consistency class")
	}
	return nil
}

func strength(class Class) uint8 {
	switch class {
	case Strong:
		return 3
	case Causal:
		return 2
	case Eventual:
		return 1
	default:
		return 0
	}
}
