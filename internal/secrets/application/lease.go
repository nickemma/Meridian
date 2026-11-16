package application

import (
	"fmt"
	"sync"
	"time"

	"github.com/nickemma/meridian/internal/secrets/domain"
	"github.com/nickemma/meridian/internal/secrets/ports"
)

// LeaseManager issues and tracks leases for secret access.
// Leases are in-memory only — they do not go through Raft
// because they are soft state. On restart, services simply
// re-fetch their secrets and get new leases.
//
// This is an intentional design decision: requiring lease
// persistence through Raft would add write amplification
// for every secret access. The tradeoff is that a node
// restart forces all services to re-authenticate — which
// is acceptable and expected behaviour.
type LeaseManager struct {
	mu     sync.RWMutex
	leases map[string]*domain.Lease // leaseID → Lease
	ids    ports.IDGenerator
}

// NewLeaseManager creates an empty lease manager.
func NewLeaseManager(ids ports.IDGenerator) *LeaseManager {
	lm := &LeaseManager{
		leases: make(map[string]*domain.Lease),
		ids:    ids,
	}
	// Start the background reaper.
	go lm.reapExpired()
	return lm
}

// Issue creates a new lease for a service accessing a secret.
func (lm *LeaseManager) Issue(
	path string,
	version uint64,
	issuedTo string,
	ttl time.Duration,
) (*domain.Lease, error) {
	if ttl < domain.MinLeaseTTL {
		return nil, fmt.Errorf("lease TTL %v is below minimum %v", ttl, domain.MinLeaseTTL)
	}
	if ttl > domain.MaxLeaseTTL {
		return nil, fmt.Errorf("lease TTL %v exceeds maximum %v", ttl, domain.MaxLeaseTTL)
	}

	now := time.Now().UTC()
	lease := &domain.Lease{
		ID:        lm.ids.NewID(),
		Path:      path,
		Version:   version,
		IssuedTo:  issuedTo,
		IssuedAt:  now,
		ExpiresAt: now.Add(ttl),
	}

	lm.mu.Lock()
	lm.leases[lease.ID] = lease
	lm.mu.Unlock()

	return lease, nil
}

// Renew extends an existing lease by its original TTL duration.
// Returns the updated lease.
// Returns error if the lease does not exist or is already expired.
func (lm *LeaseManager) Renew(leaseID string, ttl time.Duration) (*domain.Lease, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lease, exists := lm.leases[leaseID]
	if !exists {
		return nil, fmt.Errorf("lease %s not found", leaseID)
	}
	if lease.IsExpired() {
		delete(lm.leases, leaseID)
		return nil, fmt.Errorf("lease %s has expired — re-fetch the secret", leaseID)
	}

	lease.ExpiresAt = time.Now().UTC().Add(ttl)
	return lease, nil
}

// Revoke immediately invalidates a lease.
// Called when a secret is rotated — existing leases for the old
// version are revoked so services are forced to re-fetch.
func (lm *LeaseManager) Revoke(leaseID string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	delete(lm.leases, leaseID)
}

// RevokeForPath revokes all leases for a given secret path.
// Called when a secret is deleted or rotated.
func (lm *LeaseManager) RevokeForPath(path string) int {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	revoked := 0
	for id, lease := range lm.leases {
		if lease.Path == path {
			delete(lm.leases, id)
			revoked++
		}
	}
	return revoked
}

// Get returns a lease by ID.
// Returns nil if the lease does not exist or has expired.
func (lm *LeaseManager) Get(leaseID string) *domain.Lease {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	lease, exists := lm.leases[leaseID]
	if !exists || lease.IsExpired() {
		return nil
	}
	return lease
}

// ActiveCount returns the number of non-expired leases.
func (lm *LeaseManager) ActiveCount() int {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	count := 0
	for _, lease := range lm.leases {
		if !lease.IsExpired() {
			count++
		}
	}
	return count
}

// reapExpired runs in the background and removes expired leases
// from memory. Without this the leases map grows unbounded.
func (lm *LeaseManager) reapExpired() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		lm.mu.Lock()
		for id, lease := range lm.leases {
			if lease.IsExpired() {
				delete(lm.leases, id)
			}
		}
		lm.mu.Unlock()
	}
}
