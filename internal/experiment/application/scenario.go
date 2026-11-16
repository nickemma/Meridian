package application

import (
	"context"
	"fmt"
	"time"

	"github.com/nickemma/meridian/internal/experiment/domain"
	kv "github.com/nickemma/meridian/proto/kv"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Workload identities issued by the development PKI.
const (
	tenantWriter = "tenant-a"
	tenantReader = "tenant-a-reader"
)

// scenario is storage-fault-trial-v1: replication, tenant denials, leader
// crash/restart, partition, quorum loss and heal. Every step is an assertion
// on client-visible outcomes; an unexpected outcome stops the trial.
func (t *trial) scenario() error {
	all := t.nodes()
	leader, err := t.leader(all)
	if err != nil {
		return err
	}
	t.record("cluster_ready", domain.KindAssertion, time.Time{}, nil)
	c, err := t.cluster.Client(all, tenantWriter)
	if err != nil {
		return err
	}
	defer c.Close()
	strong := kv.Consistency_CONSISTENCY_STRONG
	causal := kv.Consistency_CONSISTENCY_CAUSAL
	eventual := kv.Consistency_CONSISTENCY_EVENTUAL
	strongKey := "/tenants/a/strong/item"
	if err := t.rpc("strong_write", func(ctx context.Context) error {
		_, err := put(ctx, c, strongKey, "acknowledged", strong, 1)
		return err
	}); err != nil {
		return err
	}
	if err := t.rpc("strong_read", func(ctx context.Context) error { return requireValue(ctx, c, strongKey, "acknowledged", strong) }); err != nil {
		return err
	}
	// Every public mutation and read must enforce tenant ownership before data access.
	for _, operation := range []string{"get", "put", "delete", "cas"} {
		err := t.rpc("cross_tenant_"+operation, func(ctx context.Context) error {
			key := []byte("/tenants/b/strong/private")
			switch operation {
			case "get":
				_, err = get(ctx, c, string(key), strong)
			case "put":
				_, err = put(ctx, c, string(key), "forbidden", strong, 1)
			case "delete":
				_, err = c.Delete(ctx, &kv.DeleteRequest{RequestId: "deny-delete", Key: key, Consistency: strong, Context: &kv.CausalContext{PolicyVersion: 1}})
			case "cas":
				_, err = c.CompareAndSet(ctx, &kv.CompareAndSetRequest{RequestId: "deny-cas", Key: key, Value: []byte("forbidden"), Consistency: strong, Context: &kv.CausalContext{PolicyVersion: 1}})
			}
			return err
		})
		if status.Code(err) != codes.PermissionDenied {
			return fmt.Errorf("cross-tenant %s: expected PermissionDenied, got %v", operation, err)
		}
	}
	reader, err := t.cluster.Client(all, tenantReader)
	if err != nil {
		return err
	}
	defer reader.Close()
	if err := t.rpc("read_only_read", func(ctx context.Context) error { return requireValue(ctx, reader, strongKey, "acknowledged", strong) }); err != nil {
		return err
	}
	err = t.rpc("read_only_write_denied", func(ctx context.Context) error {
		_, err := put(ctx, reader, strongKey, "forbidden", strong, 1)
		return err
	})
	if status.Code(err) != codes.PermissionDenied {
		return fmt.Errorf("read-only write was not denied: %v", err)
	}
	for _, class := range []kv.Consistency{causal, eventual} {
		name := "causal"
		version := uint64(2)
		if class == eventual {
			name = "eventual"
			version = 3
		}
		key := "/tenants/a/" + name + "/item"
		if err := t.rpc(name+"_write", func(ctx context.Context) error { _, err := put(ctx, c, key, name+"-value", class, version); return err }); err != nil {
			return err
		}
		if err := t.visible(all, key, name+"-value", class); err != nil {
			return err
		}
		t.record(name+"_replicated", domain.KindAssertion, time.Time{}, nil)
	}
	err = t.rpc("causal_missing_dependency", func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
		_, err := c.Get(ctx, &kv.GetRequest{RequestId: "missing-dependency", Key: []byte("/tenants/a/causal/item"), Consistency: causal, Context: &kv.CausalContext{RaftIndex: ^uint64(0)}})
		return err
	})
	if status.Code(err) != codes.DeadlineExceeded {
		return fmt.Errorf("causal dependency did not time out: %v", err)
	}
	// A real process kill, followed by recovery from the same Rust/Raft data dirs.
	t.record("leader_crash_"+t.cluster.NodeID(leader), domain.KindFault, time.Time{}, nil)
	t.cluster.Kill(leader)
	survivors := []int{}
	for _, i := range all {
		if i != leader {
			survivors = append(survivors, i)
		}
	}
	if _, err := t.leader(survivors); err != nil {
		return err
	}
	surviving, err := t.cluster.Client(survivors, tenantWriter)
	if err != nil {
		return err
	}
	defer surviving.Close()
	if err := t.rpc("acknowledged_value_survives_leader_crash", func(ctx context.Context) error {
		return requireValue(ctx, surviving, strongKey, "acknowledged", strong)
	}); err != nil {
		return err
	}
	if err := t.cluster.CaptureMetrics("before-restart", survivors); err != nil {
		return err
	}
	if err := t.cluster.Start(leader); err != nil {
		return err
	}
	t.record("leader_restarted", domain.KindFault, time.Time{}, nil)
	if err := t.visible(all, strongKey, "acknowledged", eventual); err != nil {
		return err
	}
	leader, err = t.leader(all)
	if err != nil {
		return err
	}
	majority := []int{}
	for _, i := range all {
		if i != leader {
			majority = append(majority, i)
		}
	}
	for _, i := range majority {
		t.cluster.BlockLink(leader, i, true)
		t.cluster.BlockLink(i, leader, true)
	}
	t.record("partition_isolates_"+t.cluster.NodeID(leader), domain.KindFault, time.Time{}, nil)
	isolated, err := t.cluster.Client([]int{leader}, tenantWriter)
	if err != nil {
		return err
	}
	defer isolated.Close()
	err = t.rpc("isolated_strong_write_cannot_acknowledge", func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
		defer cancel()
		_, err := put(ctx, isolated, "/tenants/a/strong/unacknowledged", "uncertain", strong, 1)
		return err
	})
	if err == nil {
		return fmt.Errorf("minority acknowledged a strong write without quorum")
	}
	if status.Code(err) != codes.DeadlineExceeded && status.Code(err) != codes.Unavailable {
		return fmt.Errorf("unexpected minority failure: %v", err)
	}
	if _, err := t.leader(majority); err != nil {
		return err
	}
	majorityClient, err := t.cluster.Client(majority, tenantWriter)
	if err != nil {
		return err
	}
	defer majorityClient.Close()
	if err := t.rpc("majority_strong_write", func(ctx context.Context) error {
		_, err := put(ctx, majorityClient, "/tenants/a/strong/majority", "committed", strong, 1)
		return err
	}); err != nil {
		return err
	}
	if err := t.rpc("minority_eventual_write", func(ctx context.Context) error {
		_, err := put(ctx, isolated, "/tenants/a/eventual/during-partition", "available", eventual, 3)
		return err
	}); err != nil {
		return err
	}
	if err := t.cluster.CaptureMetrics("partition", all); err != nil {
		return err
	}
	for _, i := range majority {
		t.cluster.BlockLink(leader, i, false)
		t.cluster.BlockLink(i, leader, false)
	}
	t.record("partition_healed", domain.KindFault, time.Time{}, nil)
	if err := t.visible(all, "/tenants/a/eventual/during-partition", "available", eventual); err != nil {
		return err
	}
	if err := t.visible(all, "/tenants/a/strong/majority", "committed", eventual); err != nil {
		return err
	}
	t.record("post_heal_convergence", domain.KindAssertion, time.Time{}, nil)
	// Remove quorum completely; the surviving leader cannot acknowledge a write.
	leader, err = t.leader(all)
	if err != nil {
		return err
	}
	stopped := []int{}
	for _, i := range all {
		if i != leader {
			t.cluster.Kill(i)
			stopped = append(stopped, i)
		}
	}
	t.record("quorum_lost", domain.KindFault, time.Time{}, nil)
	last, err := t.cluster.Client([]int{leader}, tenantWriter)
	if err != nil {
		return err
	}
	defer last.Close()
	err = t.rpc("quorum_loss_strong_write_cannot_acknowledge", func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
		defer cancel()
		_, err := put(ctx, last, "/tenants/a/strong/no-quorum", "uncertain", strong, 1)
		return err
	})
	if err == nil {
		return fmt.Errorf("strong write acknowledged after quorum loss")
	}
	if status.Code(err) != codes.DeadlineExceeded && status.Code(err) != codes.Unavailable {
		return fmt.Errorf("unexpected quorum loss failure: %v", err)
	}
	for _, i := range stopped {
		if err := t.cluster.Start(i); err != nil {
			return err
		}
	}
	t.record("quorum_restored", domain.KindFault, time.Time{}, nil)
	if _, err := t.leader(all); err != nil {
		return err
	}
	if err := t.rpc("committed_value_after_quorum_recovery", func(ctx context.Context) error {
		return requireValue(ctx, c, "/tenants/a/strong/majority", "committed", strong)
	}); err != nil {
		return err
	}
	if err := t.visible(all, "/tenants/a/strong/majority", "committed", eventual); err != nil {
		return err
	}
	if err := t.cluster.CaptureMetrics("final", all); err != nil {
		return err
	}

	t.record("metrics_captured", domain.KindAssertion, time.Time{}, nil)
	return nil
}
