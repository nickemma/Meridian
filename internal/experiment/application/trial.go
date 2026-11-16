// Package application runs the fixed storage fault scenario against a
// Cluster and records every RPC, fault and assertion to a Timeline.
package application

import (
	"context"
	"fmt"
	"time"

	"github.com/nickemma/meridian/client"
	"github.com/nickemma/meridian/internal/experiment/domain"
	"github.com/nickemma/meridian/internal/experiment/ports"
	kv "github.com/nickemma/meridian/proto/kv"
	"google.golang.org/grpc/status"
)

type trial struct {
	ctx       context.Context
	cluster   ports.Cluster
	timeline  ports.Timeline
	sequence  int
	recordErr error
	stats     domain.Stats
}

// Run executes the scenario on an already started cluster. It returns the
// recorded statistics even when an assertion fails.
func Run(ctx context.Context, cluster ports.Cluster, timeline ports.Timeline) (domain.Stats, error) {
	t := &trial{ctx: ctx, cluster: cluster, timeline: timeline, stats: domain.Stats{Errors: map[string]int{}}}
	err := t.scenario()
	if err == nil {
		err = t.recordErr
	}
	return t.stats, err
}

func (t *trial) nodes() []int {
	all := make([]int, t.cluster.Size())
	for i := range all {
		all[i] = i
	}
	return all
}

func (t *trial) record(name, kind string, start time.Time, err error) {
	t.sequence++
	e := domain.Event{Sequence: t.sequence, Name: name, Kind: kind, At: time.Now().UTC()}
	if kind == domain.KindRPC {
		e.Milliseconds = float64(time.Since(start)) / float64(time.Millisecond)
		e.Code = status.Code(err).String()
		t.stats.Latencies = append(t.stats.Latencies, e.Milliseconds)
		if err != nil {
			t.stats.Errors[e.Code]++
		}
	}
	if err != nil {
		e.Error = err.Error()
	}
	if t.recordErr == nil {
		t.recordErr = t.timeline.Write(e)
	}
}

func (t *trial) rpc(name string, f func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(t.ctx, 2*time.Second)
	defer cancel()
	start := time.Now()
	err := f(ctx)
	t.record(name, domain.KindRPC, start, err)
	return err
}

// leader waits for one of nodes to report Leader twice, 300ms apart.
func (t *trial) leader(nodes []int) (int, error) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && t.ctx.Err() == nil {
		for _, i := range nodes {
			isLeader, err := t.reportsLeader(i)
			if err != nil {
				return 0, err
			}
			if !isLeader {
				continue
			}
			time.Sleep(300 * time.Millisecond)
			if isLeader, err = t.reportsLeader(i); err != nil {
				return 0, err
			}
			if isLeader {
				return i, nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return 0, fmt.Errorf("no stable leader before deadline")
}

// reportsLeader asks one node for its role. Only connection setup failures
// are errors; an unreachable node is simply not the leader.
func (t *trial) reportsLeader(node int) (bool, error) {
	c, err := t.cluster.Client([]int{node}, tenantWriter)
	if err != nil {
		return false, err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(t.ctx, 250*time.Millisecond)
	defer cancel()
	s, err := c.Status(ctx)
	return err == nil && s.Role == "Leader", nil
}

// visible waits until every node serves value for key at class.
func (t *trial) visible(nodes []int, key, value string, class kv.Consistency) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && t.ctx.Err() == nil {
		all := true
		for _, i := range nodes {
			c, err := t.cluster.Client([]int{i}, tenantWriter)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(t.ctx, 300*time.Millisecond)
			err = requireValue(ctx, c, key, value, class)
			cancel()
			_ = c.Close()
			if err != nil {
				all = false
			}
		}
		if all {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("replication visibility did not converge before deadline")
}

func put(ctx context.Context, c *client.Client, key, value string, class kv.Consistency, version uint64) (*kv.PutResponse, error) {
	return c.Put(ctx, &kv.PutRequest{RequestId: fmt.Sprintf("trial-%d", time.Now().UnixNano()), Key: []byte(key), Value: []byte(value), Consistency: class, Context: &kv.CausalContext{PolicyVersion: version}})
}

func get(ctx context.Context, c *client.Client, key string, class kv.Consistency) (*kv.GetResponse, error) {
	return c.Get(ctx, &kv.GetRequest{RequestId: fmt.Sprintf("read-%d", time.Now().UnixNano()), Key: []byte(key), Consistency: class})
}

func requireValue(ctx context.Context, c *client.Client, key, value string, class kv.Consistency) error {
	r, err := get(ctx, c, key, class)
	if err != nil {
		return err
	}
	if !r.Found || string(r.Value) != value {
		return fmt.Errorf("expected acknowledged value was not visible")
	}
	return nil
}
