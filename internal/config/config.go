package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nickemma/meridian/internal/consistency"
)

// Config holds everything a Meridian node needs to know about itself
// and its cluster. Loaded once at startup — never mutated at runtime.

type Config struct {
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
	DataDir string // where the WAL and SSTables are written to disk

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

// Load reads configuration from environment variables.
// We use env vars because this node will run in Docker —
// env vars are the natural config mechanism for containerised processes.

func Load() (*Config, error) {
	nodeID := requireEnv("MERIDIAN_NODE_ID")
	raftPort := requireEnvInt("MERIDIAN_RAFT_PORT")
	clientPort := requireEnvInt("MERIDIAN_CLIENT_PORT")
	dataDir := requireEnv("MERIDIAN_DATA_DIR")

	peers, err := parsePeers(os.Getenv("MERIDIAN_PEERS"))
	if err != nil {
		return nil, fmt.Errorf("parsing peers: %w", err)
	}

	quorumSize := (len(peers)+1)/2 + 1 // majority of total nodes (peers + self)
	policies, err := parsePolicies(os.Getenv("MERIDIAN_POLICIES"))
	if err != nil {
		return nil, fmt.Errorf("parsing namespace policies: %w", err)
	}

	return &Config{
		NodeID:             nodeID,
		RaftPort:           raftPort,
		ClientPort:         clientPort,
		Peers:              peers,
		DataDir:            dataDir,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		QuorumSize:         quorumSize,
		Policies:           policies,
	}, nil
}

// parsePolicies accepts comma-separated prefix:class:version entries, for
// example "/strong/:strong:1,/causal/:causal:2,/eventual/:eventual:3". Empty
// input deliberately leaves the slice empty so Server.New installs its explicit
// development-only root strong default.
func parsePolicies(raw string) ([]consistency.NamespacePolicy, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	entries := strings.Split(raw, ",")
	policies := make([]consistency.NamespacePolicy, 0, len(entries))
	for _, entry := range entries {
		parts := strings.Split(strings.TrimSpace(entry), ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid policy %q; expected prefix:class:version", entry)
		}
		version, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil || version == 0 {
			return nil, fmt.Errorf("invalid policy version %q", parts[2])
		}
		var class consistency.Class
		switch parts[1] {
		case "strong":
			class = consistency.Strong
		case "causal":
			class = consistency.Causal
		case "eventual":
			class = consistency.Eventual
		default:
			return nil, fmt.Errorf("invalid policy class %q", parts[1])
		}
		policy := consistency.NamespacePolicy{Prefix: parts[0], Class: class, Version: version}
		if policy.Prefix != "/" && (!strings.HasPrefix(policy.Prefix, "/") || !strings.HasSuffix(policy.Prefix, "/")) {
			return nil, fmt.Errorf("invalid policy prefix %q", policy.Prefix)
		}
		policies = append(policies, policy)
	}
	return policies, nil
}

// parsePeers parses "node-2:9090,node-3:9090" into a slice of Peer.
func parsePeers(raw string) ([]Peer, error) {
	if raw == "" {
		return []Peer{}, nil
	}

	parts := strings.Split(raw, ",")
	peers := make([]Peer, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		segments := strings.SplitN(part, ":", 2)
		if len(segments) != 2 {
			return nil, fmt.Errorf("invalid peer format %q — expected host:port", part)
		}
		peers = append(peers, Peer{
			ID:      segments[0],
			Address: part,
		})
	}

	return peers, nil
}

func requireEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		fmt.Fprintf(os.Stderr, "required environment variable %s is not set\n", key)
		os.Exit(1)
	}
	return val
}

func requireEnvInt(key string) int {
	val := requireEnv(key)
	n, err := strconv.Atoi(val)
	if err != nil {
		fmt.Fprintf(os.Stderr, "environment variable %s must be an integer, got %q\n", key, val)
		os.Exit(1)
	}
	return n
}
