package secrets

import (
	"testing"
	"time"
)

func TestLease_IssuedAndValid(t *testing.T) {
	lm := NewLeaseManager()

	lease, err := lm.Issue("svc/key", 1, "payments-service", 1*time.Hour)
	if err != nil {
		t.Fatalf("expected lease, got error: %v", err)
	}
	if lease.IsExpired() {
		t.Error("new lease should not be expired")
	}
	if lease.TTLRemaining() <= 0 {
		t.Error("new lease should have positive TTL remaining")
	}
}

func TestLease_ExpiredAfterTTL(t *testing.T) {
	lm := NewLeaseManager()

	lease, _ := lm.Issue("svc/key", 1, "payments-service", 1*time.Minute)

	// Manually backdate the expiry to simulate expiration
	lease.ExpiresAt = time.Now().Add(-1 * time.Second)

	if !lease.IsExpired() {
		t.Error("lease should be expired after TTL")
	}
	if lease.TTLRemaining() != 0 {
		t.Errorf("expired lease TTL should be 0, got %v", lease.TTLRemaining())
	}
}

func TestLease_RenewExtendsTTL(t *testing.T) {
	lm := NewLeaseManager()

	lease, _ := lm.Issue("svc/key", 1, "payments-service", 1*time.Hour)
	originalExpiry := lease.ExpiresAt

	time.Sleep(10 * time.Millisecond)

	renewed, err := lm.Renew(lease.ID, 1*time.Hour)
	if err != nil {
		t.Fatalf("renew failed: %v", err)
	}
	if !renewed.ExpiresAt.After(originalExpiry) {
		t.Error("renewed lease should expire later than original")
	}
}

func TestLease_RenewExpiredLeaseErrors(t *testing.T) {
	lm := NewLeaseManager()

	lease, _ := lm.Issue("svc/key", 1, "payments-service", 1*time.Hour)

	// Force expiry
	lm.mu.Lock()
	lm.leases[lease.ID].ExpiresAt = time.Now().Add(-1 * time.Second)
	lm.mu.Unlock()

	_, err := lm.Renew(lease.ID, 1*time.Hour)
	if err == nil {
		t.Error("expected error renewing expired lease")
	}
}

func TestLease_RevokeInvalidates(t *testing.T) {
	lm := NewLeaseManager()

	lease, _ := lm.Issue("svc/key", 1, "payments-service", 1*time.Hour)
	lm.Revoke(lease.ID)

	if lm.Get(lease.ID) != nil {
		t.Error("revoked lease should not be retrievable")
	}
}

func TestLease_RevokeForPathRevokesAll(t *testing.T) {
	lm := NewLeaseManager()

	lm.Issue("svc/key", 1, "service-a", 1*time.Hour)
	lm.Issue("svc/key", 1, "service-b", 1*time.Hour)
	lm.Issue("svc/other", 1, "service-c", 1*time.Hour)

	revoked := lm.RevokeForPath("svc/key")

	if revoked != 2 {
		t.Errorf("expected 2 revoked, got %d", revoked)
	}
	if lm.ActiveCount() != 1 {
		t.Errorf("expected 1 active lease remaining, got %d", lm.ActiveCount())
	}
}

func TestLease_TTLBoundsEnforced(t *testing.T) {
	lm := NewLeaseManager()

	_, err := lm.Issue("svc/key", 1, "svc", 1*time.Second)
	if err == nil {
		t.Error("expected error for TTL below minimum")
	}

	_, err = lm.Issue("svc/key", 1, "svc", 48*time.Hour)
	if err == nil {
		t.Error("expected error for TTL above maximum")
	}
}

func TestRotation_SchedulesGracePeriod(t *testing.T) {
	s := NewStore()

	// Write initial secret
	s.Apply(makeCmd(CmdPutSecret, "svc/key", []byte("original")))

	// Rotate with a 5-minute grace period
	cmd, _ := NewRotateCommand("svc/key", []byte("rotated"), "ops", 5*time.Minute)
	s.Apply(cmd)

	// Old version should have a revocation time set
	versions, _ := s.ListVersions("svc/key")
	if len(versions) != 2 {
		t.Fatalf("expected 2 versions, got %d", len(versions))
	}

	oldVersion := versions[0]
	if oldVersion.RevokeAt.IsZero() {
		t.Error("expected old version to have RevokeAt set after rotation")
	}
	// Old version should NOT be revoked yet — grace period still active
	if oldVersion.IsRevoked() {
		t.Error("old version should still be valid during grace period")
	}
}
