// Package client provides a small leader-aware Go client for Meridian's public
// gRPC API. It is intentionally separate from internal server code so third
// parties can import it without relying on implementation packages.
package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	kv "github.com/nickemma/meridian/proto/kv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client discovers a Raft leader for strong operations. It does not retry a
// data mutation after an unavailable response because the server has no durable
// request de-duplication yet; callers must decide whether retry is safe.
type Client struct {
	targets []string
	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
}

func New(targets []string) (*Client, error) {
	if len(targets) == 0 {
		return nil, fmt.Errorf("at least one Meridian target is required")
	}
	copyTargets := make([]string, 0, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		if target == "" {
			return nil, fmt.Errorf("Meridian target is empty")
		}
		if _, exists := seen[target]; !exists {
			seen[target] = struct{}{}
			copyTargets = append(copyTargets, target)
		}
	}
	return &Client{targets: copyTargets, conns: make(map[string]*grpc.ClientConn)}, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var first error
	for target, connection := range c.conns {
		if err := connection.Close(); err != nil && first == nil {
			first = fmt.Errorf("close %s: %w", target, err)
		}
	}
	c.conns = make(map[string]*grpc.ClientConn)
	return first
}

func (c *Client) Get(ctx context.Context, request *kv.GetRequest) (*kv.GetResponse, error) {
	service, err := c.serviceFor(ctx, request.GetConsistency())
	if err != nil {
		return nil, err
	}
	return service.Get(ctx, request)
}

func (c *Client) Put(ctx context.Context, request *kv.PutRequest) (*kv.PutResponse, error) {
	service, err := c.serviceFor(ctx, request.GetConsistency())
	if err != nil {
		return nil, err
	}
	return service.Put(ctx, request)
}

func (c *Client) Delete(ctx context.Context, request *kv.DeleteRequest) (*kv.DeleteResponse, error) {
	service, err := c.serviceFor(ctx, request.GetConsistency())
	if err != nil {
		return nil, err
	}
	return service.Delete(ctx, request)
}

func (c *Client) CompareAndSet(ctx context.Context, request *kv.CompareAndSetRequest) (*kv.CompareAndSetResponse, error) {
	service, err := c.leader(ctx)
	if err != nil {
		return nil, err
	}
	return service.CompareAndSet(ctx, request)
}

func (c *Client) serviceFor(ctx context.Context, consistency kv.Consistency) (kv.KVServiceClient, error) {
	if consistency == kv.Consistency_CONSISTENCY_STRONG {
		return c.leader(ctx)
	}
	// Weak paths are locally serviceable. Choose the first configured target;
	// callers wanting locality can order targets accordingly.
	return c.service(c.targets[0])
}

func (c *Client) leader(ctx context.Context) (kv.KVServiceClient, error) {
	var lastErr error
	for _, target := range c.targets {
		service, err := c.service(target)
		if err != nil {
			lastErr = err
			continue
		}
		statusCtx, cancel := statusContext(ctx)
		response, err := service.Status(statusCtx, &kv.StatusRequest{})
		cancel()
		if err != nil {
			lastErr = fmt.Errorf("status from %s: %w", target, err)
			continue
		}
		if response.GetRole() == "Leader" {
			return service, nil
		}
	}
	if lastErr != nil {
		return nil, fmt.Errorf("discover Meridian leader: %w", lastErr)
	}
	return nil, fmt.Errorf("discover Meridian leader: no target reports Leader")
}

func (c *Client) service(target string) (kv.KVServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	connection := c.conns[target]
	if connection == nil {
		var err error
		connection, err = grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("connect to %s: %w", target, err)
		}
		c.conns[target] = connection
	}
	return kv.NewKVServiceClient(connection), nil
}

func statusContext(parent context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := parent.Deadline(); ok {
		return context.WithDeadline(parent, deadline)
	}
	return context.WithTimeout(parent, 500*time.Millisecond)
}
