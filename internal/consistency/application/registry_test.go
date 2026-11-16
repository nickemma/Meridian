package application

import (
	"errors"
	"testing"

	"github.com/nickemma/meridian/internal/consistency/domain"
)

func TestPolicyRegistryUsesLongestPrefixAndImmutablePolicies(t *testing.T) {
	registry := NewPolicyRegistry()
	if err := registry.Install(domain.NamespacePolicy{Prefix: "/", Class: domain.Eventual, Version: 1}); err != nil {
		t.Fatalf("install root policy: %v", err)
	}
	if err := registry.Install(domain.NamespacePolicy{Prefix: "/accounts/", Class: domain.Strong, Version: 2}); err != nil {
		t.Fatalf("install account policy: %v", err)
	}
	policy, err := registry.Lookup("/accounts/alice")
	if err != nil || policy.Class != domain.Strong || policy.Version != 2 {
		t.Fatalf("lookup = %+v, %v", policy, err)
	}
	if err := registry.Install(domain.NamespacePolicy{Prefix: "/accounts/", Class: domain.Eventual, Version: 3}); !errors.Is(err, domain.ErrPolicyExists) {
		t.Fatalf("replace policy error = %v, want immutable-policy error", err)
	}
}

func TestPolicyRegistryEnforcesReadAndWriteMatrix(t *testing.T) {
	registry := NewPolicyRegistry()
	if err := registry.Install(domain.NamespacePolicy{Prefix: "/profiles/", Class: domain.Causal, Version: 7}); err != nil {
		t.Fatalf("install policy: %v", err)
	}
	if _, err := registry.ValidateWrite("/profiles/a", domain.Causal, 7); err != nil {
		t.Fatalf("causal write rejected: %v", err)
	}
	if _, err := registry.ValidateWrite("/profiles/a", domain.Strong, 7); !errors.Is(err, domain.ErrWriteClass) {
		t.Fatalf("strong write error = %v, want write-class error", err)
	}
	if _, err := registry.ValidateRead("/profiles/a", domain.Eventual); err != nil {
		t.Fatalf("weaker eventual read rejected: %v", err)
	}
	if _, err := registry.ValidateRead("/profiles/a", domain.Strong); !errors.Is(err, domain.ErrReadTooStrong) {
		t.Fatalf("strong read error = %v, want read-too-strong error", err)
	}
	if _, err := registry.ValidateWrite("/profiles/a", domain.Causal, 6); err == nil {
		t.Fatal("stale policy version was accepted")
	}
}
