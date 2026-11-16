// Package domain describes everything a Meridian node needs to know about
// itself and its cluster. A Config is built once at startup and never mutated.
package domain

import (
	"time"

	consistency "github.com/nickemma/meridian/internal/consistency/domain"
	security "github.com/nickemma/meridian/internal/security/domain"
)

// Default Raft timings. Elections are randomised between the two bounds.
const (
	DefaultElectionTimeoutMin = 150 * time.Millisecond
	DefaultElectionTimeoutMax = 300 * time.Millisecond
	DefaultHeartbeatInterval  = 50 * time.Millisecond
	// MetricsPortOffset places the Prometheus listener at ClientPort+1000.
	MetricsPortOffset = 1000
)

type Config struct {
	Security security.Config

	// Identity
	NodeID string // unique name for this node e.g. "node-1"

	// Networking
	RaftPort   int    // port this node listens on for Raft RPCs (peer-to-peer)
	ClientPort int    // port this node listens on for client requests
	Peers      []Peer // the other nodes in the cluster

	// Raft tuning
	ElectionTimeoutMin time.Duration // randomised between min and max
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration

	// Storage
	DataDir string // where Raft state and the LSM engine are written

	// Cluster
	QuorumSize int // how many nodes must agree for a commit (majority)

	// Policies select a single write mechanism per immutable namespace. An empty
	// configuration receives a root strong policy for backward-compatible local
	// development; production deployments should explicitly supply policies.
	Policies []consistency.NamespacePolicy
}

// Peer represents another node in the cluster.
type Peer struct {
	ID      string // e.g. "node-2"
	Address string // e.g. "node-2:9090"
}

// PeerIDs returns the configured peer IDs in order.
func (c *Config) PeerIDs() []string {
	ids := make([]string, len(c.Peers))
	for i, p := range c.Peers {
		ids[i] = p.ID
	}
	return ids
}

// MetricsPort is where the node serves /metrics and /healthz.
func (c *Config) MetricsPort() int { return c.ClientPort + MetricsPortOffset }

// QuorumFor returns the majority of a cluster made of peerCount peers plus self.
func QuorumFor(peerCount int) int { return (peerCount+1)/2 + 1 }
