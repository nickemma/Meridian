// Package application evaluates access requests against registered policies.
package application

import (
	"fmt"
	"log"
	"sync"

	"github.com/nickemma/meridian/internal/policy/domain"
	"github.com/nickemma/meridian/internal/policy/ports"
)

// Engine is deny-by-default — a request with no matching policy is always
// denied.
//
// The engine is the WASM sandbox boundary. In this implementation policies
// are evaluated natively in Go. The interface is identical to what a WASM
// runtime would expose — the swap is behind this interface and does not
// affect callers.
type Engine struct {
	mu       sync.RWMutex
	policies ports.Repository
}

// NewEngine creates an engine over policies. All requests are denied until
// policies are registered.
func NewEngine(policies ports.Repository) *Engine {
	return &Engine{policies: policies}
}

// Register adds or replaces a policy. Safe while the engine serves requests.
func (e *Engine) Register(policy *domain.Policy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.policies.Put(policy)
	log.Printf("[policy] registered policy %q (version=%d rules=%d)",
		policy.Name, policy.Version, len(policy.Rules))
	return nil
}

// Revoke removes a policy. Requests that relied on it are then denied.
func (e *Engine) Revoke(name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.policies.Delete(name)
	log.Printf("[policy] revoked policy %q", name)
}

// Evaluate returns Allowed=true if any registered policy matches. This is
// the hot path — called on every secret access. It must be fast and must
// never panic.
func (e *Engine) Evaluate(req *domain.Request) domain.Decision {
	e.mu.RLock()
	defer e.mu.RUnlock()

	policies := e.policies.All()
	if len(policies) == 0 {
		return domain.Decision{Allowed: false, Reason: "no policies registered — deny by default"}
	}
	// First match wins.
	for _, policy := range policies {
		if allowed, _ := policy.Matches(req); allowed {
			log.Printf("[policy] ALLOW identity=%q path=%q action=%q policy=%q",
				req.Identity, req.Path, req.Action, policy.Name)
			return domain.Decision{Allowed: true, Reason: fmt.Sprintf("matched policy %q", policy.Name), Policy: policy.Name}
		}
	}
	log.Printf("[policy] DENY identity=%q path=%q action=%q — no matching policy",
		req.Identity, req.Path, req.Action)
	return domain.Decision{Allowed: false, Reason: "no policy grants this access"}
}

// List returns the names of all registered policies.
func (e *Engine) List() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	policies := e.policies.All()
	names := make([]string, 0, len(policies))
	for _, policy := range policies {
		names = append(names, policy.Name)
	}
	return names
}

// PolicyCount returns the number of registered policies.
func (e *Engine) PolicyCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.policies.All())
}
