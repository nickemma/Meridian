package config

import (
	"testing"

	"github.com/nickemma/meridian/internal/consistency"
)

func TestParsePolicies(t *testing.T) {
	policies, err := parsePolicies("/strong/:strong:1, /causal/:causal:2,/eventual/:eventual:3")
	if err != nil {
		t.Fatal(err)
	}
	if len(policies) != 3 || policies[0].Class != consistency.Strong || policies[1].Class != consistency.Causal || policies[2].Class != consistency.Eventual {
		t.Fatalf("policies = %#v", policies)
	}
}

func TestParsePoliciesRejectsMalformedEntry(t *testing.T) {
	if _, err := parsePolicies("/causal/:unknown:1"); err == nil {
		t.Fatal("parsePolicies accepted unknown class")
	}
}
