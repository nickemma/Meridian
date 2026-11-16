// Package ports declares the cluster and artifact sinks the fault scenario
// drives. Adapters supply real node processes and loopback TCP links.
package ports

import (
	"github.com/nickemma/meridian/client"
	"github.com/nickemma/meridian/internal/experiment/domain"
)

// Cluster is a fixed-size set of nodes addressed by index.
type Cluster interface {
	Size() int
	NodeID(node int) string
	// Start launches (or relaunches) a node on its existing data directory.
	Start(node int) error
	// Kill terminates a node's process without a graceful shutdown.
	Kill(node int)
	// BlockLink cuts (or restores) the one-way peer link from → to.
	BlockLink(from, to int, blocked bool)
	// Client connects as the named workload identity to the given nodes.
	Client(nodes []int, identity string) (*client.Client, error)
	// CaptureMetrics saves each node's /metrics under a phase name.
	CaptureMetrics(phase string, nodes []int) error
}

// Timeline persists events in order.
type Timeline interface {
	Write(domain.Event) error
}
