package consistency

import (
	"errors"
	"testing"
)

func TestPolicyRegistryUsesLongestPrefixAndImmutablePolicies(t *testing.T) {
	registry := NewPolicyRegistry()
	if err := registry.Install(NamespacePolicy{Prefix: "/", Class: Eventual, Version: 1}); err != nil {
		t.Fatalf("install root policy: %v", err)
	}
	if err := registry.Install(NamespacePolicy{Prefix: "/accounts/", Class: Strong, Version: 2}); err != nil {
		t.Fatalf("install account policy: %v", err)
	}
	policy, err := registry.Lookup("/accounts/alice")
	if err != nil || policy.Class != Strong || policy.Version != 2 {
		t.Fatalf("lookup = %+v, %v", policy, err)
	}
	if err := registry.Install(NamespacePolicy{Prefix: "/accounts/", Class: Eventual, Version: 3}); !errors.Is(err, ErrPolicyExists) {
		t.Fatalf("replace policy error = %v, want immutable-policy error", err)
	}
}

func TestPolicyRegistryEnforcesReadAndWriteMatrix(t *testing.T) {
	registry := NewPolicyRegistry()
	if err := registry.Install(NamespacePolicy{Prefix: "/profiles/", Class: Causal, Version: 7}); err != nil {
		t.Fatalf("install policy: %v", err)
	}
	if _, err := registry.ValidateWrite("/profiles/a", Causal, 7); err != nil {
		t.Fatalf("causal write rejected: %v", err)
	}
	if _, err := registry.ValidateWrite("/profiles/a", Strong, 7); !errors.Is(err, ErrWriteClass) {
		t.Fatalf("strong write error = %v, want write-class error", err)
	}
	if _, err := registry.ValidateRead("/profiles/a", Eventual); err != nil {
		t.Fatalf("weaker eventual read rejected: %v", err)
	}
	if _, err := registry.ValidateRead("/profiles/a", Strong); !errors.Is(err, ErrReadTooStrong) {
		t.Fatalf("strong read error = %v, want read-too-strong error", err)
	}
	if _, err := registry.ValidateWrite("/profiles/a", Causal, 6); err == nil {
		t.Fatal("stale policy version was accepted")
	}
}
