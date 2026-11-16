package adapters

import (
	"context"
	kv "github.com/nickemma/meridian/proto/kv"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
	"time"
)

func TestActualOutcomesAndLiveGauges(t *testing.T) {
	r := prometheus.NewRegistry()
	pending := 3.0
	o := New("node-test", r, func() float64 { return pending }, func() float64 { return 2 })
	_, err := o.Intercept(context.Background(), &kv.GetRequest{Consistency: kv.Consistency_CONSISTENCY_CAUSAL}, &grpc.UnaryServerInfo{FullMethod: "/kv.KVService/Get"}, func(context.Context, any) (any, error) { return nil, status.Error(codes.PermissionDenied, "denied") })
	if status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
	o.Replicated("eventual", "node-2", nil)
	o.Replicated("eventual", "node-2", status.Error(codes.Unavailable, "unavailable"))
	o.Waited(time.Now(), status.Error(codes.DeadlineExceeded, "dependency missing"))
	pending = 4
	families, err := r.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, family := range families {
		seen[family.GetName()] = true
		if family.GetName() == "meridian_causal_pending_records" && family.Metric[0].Gauge.GetValue() != 4 {
			t.Fatal("backlog gauge is not live")
		}
		if family.GetName() == "meridian_client_requests_total" {
			metric := family.Metric[0]
			if metric.Counter.GetValue() != 1 {
				t.Fatal("denial was not counted")
			}
			found := false
			for _, label := range metric.Label {
				if label.GetName() == "code" && label.GetValue() == "PermissionDenied" {
					found = true
				}
			}
			if !found {
				t.Fatal("wrong outcome label")
			}
		}
	}
	for _, name := range []string{"meridian_client_requests_total", "meridian_client_request_seconds", "meridian_dependency_wait_seconds", "meridian_replication_attempts_total", "meridian_raft_apply_gap"} {
		if !seen[name] {
			t.Fatalf("missing metric %s", name)
		}
	}
}
