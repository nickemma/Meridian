package policy

import (
	"testing"
	"time"
)

func businessHour() time.Time {
	// 10:00 UTC — within business hours
	t := time.Now().UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 10, 0, 0, 0, time.UTC)
}

func afterHours() time.Time {
	// 23:00 UTC — outside business hours
	t := time.Now().UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 23, 0, 0, 0, time.UTC)
}

func paymentsPolicy() *Policy {
	return &Policy{
		Name:    "payments-service-policy",
		Version: 1,
		Rules: []Rule{
			{
				PathPrefix: "services/payments/",
				Identities: []string{"payments-service"},
				Actions:    []Action{ActionRead, ActionList},
			},
		},
	}
}

func TestEngine_DenyByDefaultNoPolcies(t *testing.T) {
	e := NewEngine()

	decision := e.Evaluate(&Request{
		Identity:  "payments-service",
		Path:      "services/payments/db-password",
		Action:    ActionRead,
		SourceIP:  "10.0.0.1",
		Timestamp: businessHour(),
	})

	if decision.Allowed {
		t.Error("expected deny with no policies registered")
	}
}

func TestEngine_AllowsMatchingPolicy(t *testing.T) {
	e := NewEngine()
	e.Register(paymentsPolicy())

	decision := e.Evaluate(&Request{
		Identity:  "payments-service",
		Path:      "services/payments/db-password",
		Action:    ActionRead,
		SourceIP:  "10.0.0.1",
		Timestamp: businessHour(),
	})

	if !decision.Allowed {
		t.Errorf("expected allow, got deny: %s", decision.Reason)
	}
	if decision.Policy != "payments-service-policy" {
		t.Errorf("expected policy name, got %q", decision.Policy)
	}
}

func TestEngine_DeniesWrongIdentity(t *testing.T) {
	e := NewEngine()
	e.Register(paymentsPolicy())

	decision := e.Evaluate(&Request{
		Identity:  "billing-service", // not in policy
		Path:      "services/payments/db-password",
		Action:    ActionRead,
		SourceIP:  "10.0.0.1",
		Timestamp: businessHour(),
	})

	if decision.Allowed {
		t.Error("expected deny for wrong identity")
	}
}

func TestEngine_DeniesWrongAction(t *testing.T) {
	e := NewEngine()
	e.Register(paymentsPolicy())

	decision := e.Evaluate(&Request{
		Identity:  "payments-service",
		Path:      "services/payments/db-password",
		Action:    ActionDelete, // not in policy
		SourceIP:  "10.0.0.1",
		Timestamp: businessHour(),
	})

	if decision.Allowed {
		t.Error("expected deny for disallowed action")
	}
}

func TestEngine_DeniesWrongPathPrefix(t *testing.T) {
	e := NewEngine()
	e.Register(paymentsPolicy())

	decision := e.Evaluate(&Request{
		Identity:  "payments-service",
		Path:      "services/billing/db-password", // wrong prefix
		Action:    ActionRead,
		SourceIP:  "10.0.0.1",
		Timestamp: businessHour(),
	})

	if decision.Allowed {
		t.Error("expected deny for path outside allowed prefix")
	}
}

func TestEngine_DeniesAfterHours(t *testing.T) {
	e := NewEngine()
	e.Register(&Policy{
		Name:    "business-hours-only",
		Version: 1,
		Rules: []Rule{
			{
				PathPrefix:        "services/payments/",
				Identities:        []string{"payments-service"},
				Actions:           []Action{ActionRead},
				BusinessHoursOnly: true,
			},
		},
	})

	decision := e.Evaluate(&Request{
		Identity:  "payments-service",
		Path:      "services/payments/db-password",
		Action:    ActionRead,
		SourceIP:  "10.0.0.1",
		Timestamp: afterHours(),
	})

	if decision.Allowed {
		t.Error("expected deny outside business hours")
	}
}

func TestEngine_DeniesDisallowedCIDR(t *testing.T) {
	e := NewEngine()
	e.Register(&Policy{
		Name:    "datacenter-only",
		Version: 1,
		Rules: []Rule{
			{
				PathPrefix:   "services/payments/",
				Identities:   []string{"payments-service"},
				Actions:      []Action{ActionRead},
				AllowedCIDRs: []string{"10.0.0.0/8"},
			},
		},
	})

	decision := e.Evaluate(&Request{
		Identity:  "payments-service",
		Path:      "services/payments/db-password",
		Action:    ActionRead,
		SourceIP:  "203.0.113.1", // public IP — not in datacenter CIDR
		Timestamp: businessHour(),
	})

	if decision.Allowed {
		t.Error("expected deny for IP outside allowed CIDR")
	}
}

func TestEngine_AllowsFromDatacenterCIDR(t *testing.T) {
	e := NewEngine()
	e.Register(&Policy{
		Name:    "datacenter-only",
		Version: 1,
		Rules: []Rule{
			{
				PathPrefix:   "services/payments/",
				Identities:   []string{"payments-service"},
				Actions:      []Action{ActionRead},
				AllowedCIDRs: []string{"10.0.0.0/8"},
			},
		},
	})

	decision := e.Evaluate(&Request{
		Identity:  "payments-service",
		Path:      "services/payments/db-password",
		Action:    ActionRead,
		SourceIP:  "10.20.30.40", // inside datacenter CIDR
		Timestamp: businessHour(),
	})

	if !decision.Allowed {
		t.Errorf("expected allow from datacenter IP, got: %s", decision.Reason)
	}
}

func TestEngine_RevokeRemovesPolicy(t *testing.T) {
	e := NewEngine()
	e.Register(paymentsPolicy())
	e.Revoke("payments-service-policy")

	decision := e.Evaluate(&Request{
		Identity:  "payments-service",
		Path:      "services/payments/db-password",
		Action:    ActionRead,
		SourceIP:  "10.0.0.1",
		Timestamp: businessHour(),
	})

	if decision.Allowed {
		t.Error("expected deny after policy revocation")
	}
}

func TestEngine_RegisterRejectsEmptyRules(t *testing.T) {
	e := NewEngine()

	err := e.Register(&Policy{
		Name:  "empty-policy",
		Rules: []Rule{},
	})

	if err == nil {
		t.Error("expected error registering policy with no rules")
	}
}
