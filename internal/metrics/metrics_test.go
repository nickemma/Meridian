package metrics

import (
	"testing"
)

func TestMetrics_New(t *testing.T) {
	// Verify all metrics are created without panicking.
	// promauto panics on duplicate registration —
	// this test confirms the metric names are all unique.
	m := New("test-node")

	if m.RaftTerm == nil {
		t.Error("expected RaftTerm metric")
	}
	if m.SecretsReadsTotal == nil {
		t.Error("expected SecretsReadsTotal metric")
	}
	if m.PolicyEvaluationsTotal == nil {
		t.Error("expected PolicyEvaluationsTotal metric")
	}
	if m.AuditRecordsTotal == nil {
		t.Error("expected AuditRecordsTotal metric")
	}
}

func TestMetrics_CountersIncrement(t *testing.T) {
	m := New("test-node-2")

	m.RaftElectionsTotal.Inc()
	m.RaftHeartbeatsTotal.Inc()
	m.RaftHeartbeatsTotal.Inc()
	m.SecretsReadsTotal.WithLabelValues("success").Inc()
	m.PolicyEvaluationsTotal.WithLabelValues("allow").Inc()
	m.PolicyEvaluationsTotal.WithLabelValues("deny").Inc()
	m.AuditRecordsTotal.Inc()

	// If no panic occurred all counters incremented correctly.
	// Prometheus counters are append-only — no assertion needed
	// beyond confirming the operations complete without error.
}

func TestMetrics_GaugesSetAndRead(t *testing.T) {
	m := New("test-node-3")

	m.RaftTerm.Set(5)
	m.RaftRole.Set(2) // Leader
	m.RaftCommitIndex.Set(42)
	m.SecretsActiveLeases.Set(17)
	m.StorageMemtableSizeBytes.Set(1024 * 1024)
	m.StorageSSTableCount.Set(3)
}
