package policy

import (
	"fmt"
	"log"
	"sync"
)

// Engine manages a set of policies and evaluates access requests.
// Deny-by-default — a request with no matching policy is always denied.
//
// The engine is the WASM sandbox boundary. In this implementation
// policies are evaluated natively in Go. The interface is identical
// to what a WASM runtime would expose — the swap is behind this
// interface and does not affect callers.
type Engine struct {
	mu       sync.RWMutex
	policies map[string]*Policy // policy name → policy
}

// NewEngine creates a policy engine with no policies loaded.
// All requests are denied until policies are registered.
func NewEngine() *Engine {
	return &Engine{
		policies: make(map[string]*Policy),
	}
}

// Register adds or replaces a policy in the engine.
// Thread-safe — can be called while the engine is serving requests.
func (e *Engine) Register(policy *Policy) error {
	if policy.Name == "" {
		return fmt.Errorf("policy name cannot be empty")
	}
	if len(policy.Rules) == 0 {
		return fmt.Errorf("policy %q has no rules — would deny all requests", policy.Name)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.policies[policy.Name] = policy

	log.Printf("[policy] registered policy %q (version=%d rules=%d)",
		policy.Name, policy.Version, len(policy.Rules))
	return nil
}

// Revoke removes a policy from the engine.
// After revocation, requests that relied on this policy are denied.
func (e *Engine) Revoke(name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.policies, name)
	log.Printf("[policy] revoked policy %q", name)
}

// Evaluate checks whether a request is allowed by any registered policy.
// Returns a Decision with Allowed=true if any policy matches.
// Returns a Decision with Allowed=false if no policy matches or
// all matching policies deny the request.
//
// This is the hot path — called on every secret access.
// It must be fast and must never panic.
func (e *Engine) Evaluate(req *Request) Decision {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(e.policies) == 0 {
		return Decision{
			Allowed: false,
			Reason:  "no policies registered — deny by default",
			Policy:  "",
		}
	}

	// Evaluate all policies. First match wins.
	for _, policy := range e.policies {
		allowed, reason := policy.Matches(req)
		if allowed {
			log.Printf("[policy] ALLOW identity=%q path=%q action=%q policy=%q",
				req.Identity, req.Path, req.Action, policy.Name)
			return Decision{
				Allowed: true,
				Reason:  fmt.Sprintf("matched policy %q", policy.Name),
				Policy:  policy.Name,
			}
		}
		// Log why this policy did not match at debug level
		_ = reason
	}

	log.Printf("[policy] DENY identity=%q path=%q action=%q — no matching policy",
		req.Identity, req.Path, req.Action)

	return Decision{
		Allowed: false,
		Reason:  "no policy grants this access",
		Policy:  "",
	}
}

// List returns the names of all registered policies.
func (e *Engine) List() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	names := make([]string, 0, len(e.policies))
	for name := range e.policies {
		names = append(names, name)
	}
	return names
}

// PolicyCount returns the number of registered policies.
func (e *Engine) PolicyCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.policies)
}
