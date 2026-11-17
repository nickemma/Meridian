package adapters

import (
	"fmt"
	"net"
	"path/filepath"
	"testing"
)

// Before a node starts, its ports must stay reserved: relays and other
// listeners created in the meantime must not be able to take them.
func TestLocalClusterReservesNodePortsUntilStart(t *testing.T) {
	cluster, err := NewLocalCluster(t.TempDir(), filepath.Join(t.TempDir(), "unused-binary"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	seen := map[int]string{}
	claim := func(port int, owner string) {
		if previous, dup := seen[port]; dup {
			t.Fatalf("port %d used by both %s and %s", port, previous, owner)
		}
		seen[port] = owner
	}
	for _, n := range cluster.nodes {
		claim(n.peerPort, n.id+" peer")
		claim(n.clientPort, n.id+" client")
		claim(n.metricsPort(), n.id+" metrics")
	}
	for i := range cluster.links {
		for j, link := range cluster.links[i] {
			if link != nil {
				claim(port(link.listener), fmt.Sprintf("relay %d->%d", i, j))
			}
		}
	}
	for _, n := range cluster.nodes {
		for _, p := range []int{n.peerPort, n.clientPort, n.metricsPort()} {
			if l, err := net.Listen("tcp", fmt.Sprintf(":%d", p)); err == nil {
				_ = l.Close()
				t.Fatalf("%s port %d was not reserved before start", n.id, p)
			}
		}
	}
}
