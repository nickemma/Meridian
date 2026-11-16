// Package application builds a validated node Config from a settings Source.
package application

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/nickemma/meridian/internal/config/domain"
	"github.com/nickemma/meridian/internal/config/ports"
	consistency "github.com/nickemma/meridian/internal/consistency/domain"
	security "github.com/nickemma/meridian/internal/security/domain"
)

// Setting names. Nodes run in containers, where environment variables are the
// natural configuration mechanism.
const (
	NodeID         = "MERIDIAN_NODE_ID"
	RaftPort       = "MERIDIAN_RAFT_PORT"
	ClientPort     = "MERIDIAN_CLIENT_PORT"
	DataDir        = "MERIDIAN_DATA_DIR"
	Peers          = "MERIDIAN_PEERS"
	Policies       = "MERIDIAN_POLICIES"
	TLSCA          = "MERIDIAN_TLS_CA"
	TLSCert        = "MERIDIAN_TLS_CERT"
	TLSKey         = "MERIDIAN_TLS_KEY"
	ClientPolicies = "MERIDIAN_CLIENT_POLICIES"
)

// Load reads a complete Config. A missing required setting is an error rather
// than a partially configured node.
func Load(source ports.Source) (*domain.Config, error) {
	settings := reader{source: source}
	nodeID := settings.required(NodeID)
	raftPort := settings.requiredInt(RaftPort)
	clientPort := settings.requiredInt(ClientPort)
	dataDir := settings.required(DataDir)
	if settings.err != nil {
		return nil, settings.err
	}

	peers, err := ParsePeers(settings.optional(Peers))
	if err != nil {
		return nil, fmt.Errorf("parsing peers: %w", err)
	}
	policies, err := consistency.ParsePolicies(settings.optional(Policies))
	if err != nil {
		return nil, fmt.Errorf("parsing namespace policies: %w", err)
	}

	return &domain.Config{
		NodeID: nodeID,
		Security: security.Config{
			CAFile:      settings.optional(TLSCA),
			CertFile:    settings.optional(TLSCert),
			KeyFile:     settings.optional(TLSKey),
			ClientsFile: settings.optional(ClientPolicies),
		},
		RaftPort:           raftPort,
		ClientPort:         clientPort,
		Peers:              peers,
		DataDir:            dataDir,
		ElectionTimeoutMin: domain.DefaultElectionTimeoutMin,
		ElectionTimeoutMax: domain.DefaultElectionTimeoutMax,
		HeartbeatInterval:  domain.DefaultHeartbeatInterval,
		QuorumSize:         domain.QuorumFor(len(peers)),
		Policies:           policies,
	}, nil
}

// ParsePeers parses "node-2:9090,node-3=127.0.0.1:9092". Without an explicit
// "id=" prefix the host doubles as the peer ID.
func ParsePeers(raw string) ([]domain.Peer, error) {
	if raw == "" {
		return []domain.Peer{}, nil
	}
	parts := strings.Split(raw, ",")
	peers := make([]domain.Peer, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		id, address, explicit := strings.Cut(part, "=")
		if !explicit {
			address = part
		}
		host, _, found := strings.Cut(address, ":")
		if !found {
			return nil, fmt.Errorf("invalid peer format %q — expected host:port", part)
		}
		if !explicit {
			id = host
		}
		peers = append(peers, domain.Peer{ID: id, Address: address})
	}
	return peers, nil
}

// reader records the first missing or malformed required setting.
type reader struct {
	source ports.Source
	err    error
}

func (r *reader) optional(key string) string {
	value, _ := r.source.Lookup(key)
	return value
}

func (r *reader) required(key string) string {
	value := r.optional(key)
	if value == "" && r.err == nil {
		r.err = fmt.Errorf("required environment variable %s is not set", key)
	}
	return value
}

func (r *reader) requiredInt(key string) int {
	value := r.required(key)
	if value == "" {
		return 0
	}
	n, err := strconv.Atoi(value)
	if err != nil && r.err == nil {
		r.err = fmt.Errorf("environment variable %s must be an integer, got %q", key, value)
	}
	return n
}
