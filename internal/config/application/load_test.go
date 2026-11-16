package application

import (
	"strings"
	"testing"

	consistency "github.com/nickemma/meridian/internal/consistency/domain"
)

type mapSource map[string]string

func (m mapSource) Lookup(key string) (string, bool) {
	value, ok := m[key]
	return value, ok
}

func TestParsePeersWithExplicitIdentity(t *testing.T) {
	peers, err := ParsePeers("node-2=127.0.0.1:9092,node-3:9090")
	if err != nil || len(peers) != 2 || peers[0].ID != "node-2" || peers[0].Address != "127.0.0.1:9092" || peers[1].ID != "node-3" {
		t.Fatalf("peers=%#v error=%v", peers, err)
	}
	if _, err := ParsePeers("node-2"); err == nil {
		t.Fatal("peer without port accepted")
	}
}

func TestLoadBuildsCompleteConfig(t *testing.T) {
	cfg, err := Load(mapSource{
		NodeID: "node-1", RaftPort: "9090", ClientPort: "8080", DataDir: "/data",
		Peers: "node-2:9090,node-3:9090", Policies: "/causal/:causal:2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.QuorumSize != 2 || cfg.MetricsPort() != 9080 || len(cfg.Policies) != 1 || cfg.Policies[0].Class != consistency.Causal || cfg.Security.Enabled() {
		t.Fatalf("config = %+v", cfg)
	}
}

func TestLoadRejectsMissingOrMalformedSettings(t *testing.T) {
	complete := mapSource{NodeID: "node-1", RaftPort: "9090", ClientPort: "8080", DataDir: "/data"}
	for _, test := range []struct {
		key, value, want string
	}{
		{NodeID, "", NodeID + " is not set"},
		{RaftPort, "x", RaftPort + " must be an integer"},
		{Policies, "/causal/:unknown:1", "namespace policies"},
	} {
		source := mapSource{}
		for k, v := range complete {
			source[k] = v
		}
		source[test.key] = test.value
		if _, err := Load(source); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("Load with %s=%q error = %v, want %q", test.key, test.value, err, test.want)
		}
	}
}
