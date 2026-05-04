package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all Prometheus metrics for a Meridian node.
// Created once at startup and passed to the components that need it.
type Metrics struct {
	// --- Raft metrics ---

	// RaftTerm tracks the current Raft term.
	// Rising sharply means frequent leader elections — unhealthy.
	RaftTerm prometheus.Gauge

	// RaftRole tracks this node's current role.
	// 0=Follower, 1=Candidate, 2=Leader
	RaftRole prometheus.Gauge

	// RaftCommitIndex tracks the highest committed log index.
	// Should increase steadily under load.
	RaftCommitIndex prometheus.Gauge

	// RaftLogEntries tracks total entries in the log.
	RaftLogEntries prometheus.Gauge

	// RaftElectionsTotal counts elections started by this node.
	// High count on a stable cluster means election instability.
	RaftElectionsTotal prometheus.Counter

	// RaftHeartbeatsTotal counts heartbeats sent (leader) or received (follower).
	RaftHeartbeatsTotal prometheus.Counter

	// --- Secret operation metrics ---

	// SecretsReadsTotal counts secret read attempts, labelled by outcome.
	SecretsReadsTotal *prometheus.CounterVec

	// SecretsWritesTotal counts secret write attempts, labelled by outcome.
	SecretsWritesTotal *prometheus.CounterVec

	// SecretsRotationsTotal counts secret rotations.
	SecretsRotationsTotal prometheus.Counter

	// SecretsActiveleases tracks current live leases.
	SecretsActiveLeases prometheus.Gauge

	// SecretOperationDuration measures how long secret operations take.
	SecretOperationDuration *prometheus.HistogramVec

	// --- Policy engine metrics ---

	// PolicyEvaluationsTotal counts policy evaluations, labelled by decision.
	PolicyEvaluationsTotal *prometheus.CounterVec

	// PolicyEvaluationDuration measures policy evaluation latency.
	PolicyEvaluationDuration prometheus.Histogram

	// --- Storage engine metrics ---

	// StorageWALWritesTotal counts WAL append operations.
	StorageWALWritesTotal prometheus.Counter

	// StorageMemtableSizeBytes tracks current memtable size.
	StorageMemtableSizeBytes prometheus.Gauge

	// StorageSSTableCount tracks number of SSTables on disk.
	StorageSSTableCount prometheus.Gauge

	// --- Audit metrics ---

	// AuditRecordsTotal counts audit records written.
	AuditRecordsTotal prometheus.Counter
}

// New creates and registers all Meridian metrics with Prometheus.
// Uses promauto so metrics are registered automatically.
func New(nodeID string) *Metrics {
	labels := prometheus.Labels{"node": nodeID}

	return &Metrics{
		// Raft
		RaftTerm: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "meridian_raft_current_term",
			Help:        "Current Raft term for this node.",
			ConstLabels: labels,
		}),
		RaftRole: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "meridian_raft_role",
			Help:        "Current role: 0=Follower 1=Candidate 2=Leader.",
			ConstLabels: labels,
		}),
		RaftCommitIndex: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "meridian_raft_commit_index",
			Help:        "Highest log index known to be committed.",
			ConstLabels: labels,
		}),
		RaftLogEntries: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "meridian_raft_log_entries_total",
			Help:        "Total entries in the Raft log.",
			ConstLabels: labels,
		}),
		RaftElectionsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "meridian_raft_elections_total",
			Help:        "Total elections started by this node.",
			ConstLabels: labels,
		}),
		RaftHeartbeatsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "meridian_raft_heartbeats_total",
			Help:        "Total heartbeats sent or received.",
			ConstLabels: labels,
		}),

		// Secrets
		SecretsReadsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "meridian_secrets_reads_total",
			Help:        "Total secret read attempts by outcome.",
			ConstLabels: labels,
		}, []string{"outcome"}),
		SecretsWritesTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "meridian_secrets_writes_total",
			Help:        "Total secret write attempts by outcome.",
			ConstLabels: labels,
		}, []string{"outcome"}),
		SecretsRotationsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "meridian_secrets_rotations_total",
			Help:        "Total secret rotations performed.",
			ConstLabels: labels,
		}),
		SecretsActiveLeases: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "meridian_secrets_active_leases",
			Help:        "Current number of active secret leases.",
			ConstLabels: labels,
		}),
		SecretOperationDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "meridian_secret_operation_duration_seconds",
			Help:        "Duration of secret operations in seconds.",
			ConstLabels: labels,
			Buckets:     prometheus.DefBuckets,
		}, []string{"operation"}),

		// Policy
		PolicyEvaluationsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "meridian_policy_evaluations_total",
			Help:        "Total policy evaluations by decision.",
			ConstLabels: labels,
		}, []string{"decision"}),
		PolicyEvaluationDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:        "meridian_policy_evaluation_duration_seconds",
			Help:        "Duration of policy evaluation in seconds.",
			ConstLabels: labels,
			Buckets:     []float64{0.0001, 0.0005, 0.001, 0.005, 0.01},
		}),

		// Storage
		StorageWALWritesTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "meridian_storage_wal_writes_total",
			Help:        "Total WAL append operations.",
			ConstLabels: labels,
		}),
		StorageMemtableSizeBytes: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "meridian_storage_memtable_size_bytes",
			Help:        "Current memtable size in bytes.",
			ConstLabels: labels,
		}),
		StorageSSTableCount: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "meridian_storage_sstable_count",
			Help:        "Number of SSTable files on disk.",
			ConstLabels: labels,
		}),

		// Audit
		AuditRecordsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "meridian_audit_records_total",
			Help:        "Total audit records written.",
			ConstLabels: labels,
		}),
	}
}

// Handler returns the HTTP handler for the /metrics endpoint.
// Mount this on the node's HTTP server.
func Handler() http.Handler {
	return promhttp.Handler()
}
