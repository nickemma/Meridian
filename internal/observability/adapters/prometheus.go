// Package adapters instruments real storage RPCs with Prometheus series and a
// gRPC interceptor, without key/value labels.
package adapters

import (
	"context"
	"time"

	consistency "github.com/nickemma/meridian/internal/consistency/adapters"
	"github.com/nickemma/meridian/internal/observability/domain"
	"github.com/nickemma/meridian/internal/observability/ports"
	kv "github.com/nickemma/meridian/proto/kv"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type Observer struct {
	Requests       *prometheus.CounterVec
	Duration       *prometheus.HistogramVec
	Replication    *prometheus.CounterVec
	DependencyWait *prometheus.HistogramVec
}

// New registers the storage series and live gauges with registry.
func New(node string, registry prometheus.Registerer, pending func() float64, gap func() float64) *Observer {
	labels := prometheus.Labels{"node": node}
	o := &Observer{
		Requests:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "meridian_client_requests_total", Help: "Client RPC outcomes, including authorization denials.", ConstLabels: labels}, []string{"method", "class", "code"}),
		Duration:       prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "meridian_client_request_seconds", Help: "Client RPC duration including admission and dependency wait.", ConstLabels: labels}, []string{"method", "class"}),
		Replication:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "meridian_replication_attempts_total", Help: "Outbound weak-path RPC outcomes; success means peer acknowledgement, not global convergence.", ConstLabels: labels}, []string{"class", "peer", "outcome"}),
		DependencyWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "meridian_dependency_wait_seconds", Help: "Time spent in causal operations initially blocked on dependencies.", ConstLabels: labels}, []string{"code"}),
	}
	registry.MustRegister(o.Requests, o.Duration, o.Replication, o.DependencyWait)
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "meridian_causal_pending_records", Help: "Received records waiting for causal dependencies.", ConstLabels: labels}, pending))
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "meridian_raft_apply_gap", Help: "Local commit index minus local applied index, not remote replica lag.", ConstLabels: labels}, gap))
	return o
}

var (
	_ ports.ReplicationObserver = (*Observer)(nil)
	_ ports.DependencyObserver  = (*Observer)(nil)
)

// Intercept records every client RPC's outcome and duration.
func (o *Observer) Intercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	if o == nil {
		return next(ctx, req)
	}
	class := domain.ClassNone
	if r, ok := req.(interface{ GetConsistency() kv.Consistency }); ok {
		class = domain.ClassLabel(consistency.ClassFromProto(r.GetConsistency()))
	}
	start := time.Now()
	response, err := next(ctx, req)
	o.Requests.WithLabelValues(info.FullMethod, class, status.Code(err).String()).Inc()
	o.Duration.WithLabelValues(info.FullMethod, class).Observe(time.Since(start).Seconds())
	return response, err
}

// Replicated records one outbound replication attempt.
func (o *Observer) Replicated(class, peer string, err error) {
	if o != nil {
		o.Replication.WithLabelValues(class, peer, domain.Outcome(err)).Inc()
	}
}

// Waited records a causal dependency wait.
func (o *Observer) Waited(start time.Time, err error) {
	if o != nil {
		o.DependencyWait.WithLabelValues(status.Code(err).String()).Observe(time.Since(start).Seconds())
	}
}
