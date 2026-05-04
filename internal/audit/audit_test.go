package audit

import (
	"testing"
)

func TestAuditLog_AppendAndRetrieve(t *testing.T) {
	log := NewLog()

	record := log.Append(
		EventSecretRead,
		"payments-service",
		"services/payments/db-password",
		"success",
		map[string]string{"version": "3"},
		"trace-abc-123",
	)

	if record.Sequence != 0 {
		t.Errorf("expected sequence 0, got %d", record.Sequence)
	}
	if record.Hash == "" {
		t.Error("expected non-empty hash")
	}
	if record.PrevHash == "" {
		t.Error("expected non-empty prev_hash")
	}
	if log.Len() != 1 {
		t.Errorf("expected 1 record, got %d", log.Len())
	}
}

func TestAuditLog_ChainIsValid(t *testing.T) {
	log := NewLog()

	log.Append(EventSecretWrite, "svc-a", "svc/key", "success", nil, "")
	log.Append(EventSecretRead, "svc-b", "svc/key", "success", nil, "")
	log.Append(EventPolicyDeny, "svc-c", "svc/key", "denied", nil, "")

	if err := log.Verify(); err != nil {
		t.Errorf("expected valid chain, got: %v", err)
	}
}

func TestAuditLog_TamperingDetected(t *testing.T) {
	log := NewLog()

	log.Append(EventSecretWrite, "svc-a", "svc/key", "success", nil, "")
	log.Append(EventSecretRead, "svc-b", "svc/key", "success", nil, "")

	// Tamper with the first record — change the actor
	log.mu.Lock()
	log.records[0].Actor = "attacker" // modify after the fact
	log.mu.Unlock()

	err := log.Verify()
	if err == nil {
		t.Error("expected tamper detection, chain verified as valid")
	}
}

func TestAuditLog_SequenceIsMonotonic(t *testing.T) {
	log := NewLog()

	for i := 0; i < 5; i++ {
		r := log.Append(EventSecretRead, "svc", "path", "success", nil, "")
		if r.Sequence != uint64(i) {
			t.Errorf("expected sequence %d, got %d", i, r.Sequence)
		}
	}
}

func TestAuditLog_PrevHashLinks(t *testing.T) {
	log := NewLog()

	r0 := log.Append(EventSecretWrite, "svc", "path", "success", nil, "")
	r1 := log.Append(EventSecretRead, "svc", "path", "success", nil, "")

	// r1's prev_hash must equal r0's hash
	if r1.PrevHash != r0.Hash {
		t.Errorf("expected r1.PrevHash=%q to equal r0.Hash=%q",
			r1.PrevHash, r0.Hash)
	}
}

func TestAuditLog_SinceReturnsCorrectSlice(t *testing.T) {
	log := NewLog()

	for i := 0; i < 5; i++ {
		log.Append(EventSecretRead, "svc", "path", "success", nil, "")
	}

	records := log.Since(3)
	if len(records) != 2 {
		t.Errorf("expected 2 records since sequence 3, got %d", len(records))
	}
	if records[0].Sequence != 3 {
		t.Errorf("expected first record sequence 3, got %d", records[0].Sequence)
	}
}

func TestAuditLog_EmptyLogVerifies(t *testing.T) {
	log := NewLog()
	if err := log.Verify(); err != nil {
		t.Errorf("empty log should verify cleanly, got: %v", err)
	}
}

func TestAuditLog_MetadataPreserved(t *testing.T) {
	log := NewLog()

	meta := map[string]string{
		"version": "3",
		"policy":  "payments-policy",
	}

	r := log.Append(EventPolicyAllow, "svc", "path", "success", meta, "")

	if r.Metadata["version"] != "3" {
		t.Errorf("expected metadata version '3', got %q", r.Metadata["version"])
	}
	if r.Metadata["policy"] != "payments-policy" {
		t.Errorf("expected metadata policy 'payments-policy', got %q",
			r.Metadata["policy"])
	}
}
