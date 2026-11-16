package domain

import "testing"

func TestParsePolicies(t *testing.T) {
	policies, err := ParsePolicies("/strong/:strong:1, /causal/:causal:2,/eventual/:eventual:3")
	if err != nil {
		t.Fatal(err)
	}
	if len(policies) != 3 || policies[0].Class != Strong || policies[1].Class != Causal || policies[2].Class != Eventual {
		t.Fatalf("policies = %#v", policies)
	}
}

func TestParsePoliciesRejectsMalformedEntry(t *testing.T) {
	for _, raw := range []string{"/causal/:unknown:1", "/causal/:causal:0", "causal/:causal:1", "/causal/:causal"} {
		if _, err := ParsePolicies(raw); err == nil {
			t.Fatalf("ParsePolicies(%q) succeeded", raw)
		}
	}
}

func TestPolicyValidation(t *testing.T) {
	if err := (NamespacePolicy{Prefix: "/", Class: Eventual, Version: 1}).Validate(); err != nil {
		t.Fatalf("root policy rejected: %v", err)
	}
	for _, policy := range []NamespacePolicy{
		{Prefix: "/a", Class: Strong, Version: 1},
		{Prefix: "/a/", Class: Strong},
		{Prefix: "/a/", Class: 9, Version: 1},
	} {
		if err := policy.Validate(); err == nil {
			t.Fatalf("invalid policy accepted: %+v", policy)
		}
	}
}
