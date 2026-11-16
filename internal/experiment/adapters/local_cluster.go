// Package adapters runs the fault trial on real local node processes with
// Rust-backed disks, mTLS, and loopback TCP relays between every peer pair.
package adapters

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nickemma/meridian/client"
	config "github.com/nickemma/meridian/internal/config/application"
	configdomain "github.com/nickemma/meridian/internal/config/domain"
	"github.com/nickemma/meridian/internal/experiment/ports"
	security "github.com/nickemma/meridian/internal/security/adapters"
	"google.golang.org/grpc/credentials"
)

const clusterSize = 3

type node struct {
	id                   string
	peerPort, clientPort int
	process              *exec.Cmd
	data                 string
}

func (n *node) metricsPort() int { return n.clientPort + configdomain.MetricsPortOffset }

// LocalCluster is three node processes. links[i][j] relays i's outbound peer
// traffic to j, so a partition is a set of blocked relays.
type LocalCluster struct {
	dir, binary, pki string
	nodes            [clusterSize]*node
	links            [clusterSize][clusterSize]*relay
	credsMu          sync.Mutex
	creds            map[string]credentials.TransportCredentials
}

var _ ports.Cluster = (*LocalCluster)(nil)

// NewLocalCluster allocates ports and relays and generates a fresh
// development PKI under dir. It does not start any process.
func NewLocalCluster(dir, binary string) (*LocalCluster, error) {
	c := &LocalCluster{dir: dir, binary: binary, pki: filepath.Join(dir, "pki"), creds: map[string]credentials.TransportCredentials{}}
	if err := security.GenerateDevelopmentPKI(c.pki); err != nil {
		return nil, err
	}
	used := map[int]bool{}
	for i := range c.nodes {
		peerPort, err := freePort()
		for err == nil && used[peerPort] {
			peerPort, err = freePort()
		}
		if err != nil {
			return nil, err
		}
		used[peerPort] = true
		clientPort, err := clientAndMetricsPort(used)
		if err != nil {
			return nil, err
		}
		c.nodes[i] = &node{id: fmt.Sprintf("node-%d", i+1), peerPort: peerPort, clientPort: clientPort, data: filepath.Join(dir, fmt.Sprintf("node-%d-data", i+1))}
	}
	for i := range c.nodes {
		for j := range c.nodes {
			if i == j {
				continue
			}
			link, err := newRelay(fmt.Sprintf("127.0.0.1:%d", c.nodes[j].peerPort))
			if err != nil {
				c.Close()
				return nil, err
			}
			c.links[i][j] = link
		}
	}
	return c, nil
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// clientAndMetricsPort finds a free client port whose metrics port
// (client+offset) is also free, and reserves both.
func clientAndMetricsPort(used map[int]bool) (int, error) {
	offset := configdomain.MetricsPortOffset
	for attempts := 0; attempts < 100; attempts++ {
		p, err := freePort()
		if err != nil {
			return 0, err
		}
		if p >= 65535-offset || used[p] || used[p+offset] {
			continue
		}
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p+offset))
		if err != nil {
			continue
		}
		_ = l.Close()
		used[p] = true
		used[p+offset] = true
		return p, nil
	}
	return 0, fmt.Errorf("no available client/metrics ports")
}

func (c *LocalCluster) Size() int                            { return clusterSize }
func (c *LocalCluster) NodeID(i int) string                  { return c.nodes[i].id }
func (c *LocalCluster) BlockLink(from, to int, blocked bool) { c.links[from][to].block(blocked) }

// Start launches node i with a clean MERIDIAN_* environment.
func (c *LocalCluster) Start(i int) error {
	n := c.nodes[i]
	peers := []string{}
	for j := range c.nodes {
		if i != j {
			peers = append(peers, fmt.Sprintf("%s=%s", c.nodes[j].id, c.links[i][j].listener.Addr()))
		}
	}
	env := []string{}
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "MERIDIAN_") {
			env = append(env, entry)
		}
	}
	env = append(env,
		config.NodeID+"="+n.id,
		fmt.Sprintf("%s=%d", config.RaftPort, n.peerPort),
		fmt.Sprintf("%s=%d", config.ClientPort, n.clientPort),
		config.DataDir+"="+n.data,
		config.Peers+"="+strings.Join(peers, ","),
		config.Policies+"="+tenantPolicies(),
		config.TLSCA+"="+filepath.Join(c.pki, "ca.pem"),
		config.TLSCert+"="+filepath.Join(c.pki, n.id+".pem"),
		config.TLSKey+"="+filepath.Join(c.pki, n.id+".key"),
		config.ClientPolicies+"="+filepath.Join(c.pki, "clients.json"),
	)
	log, err := os.OpenFile(filepath.Join(c.dir, n.id+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer log.Close()
	cmd := exec.Command(c.binary)
	cmd.Env = env
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		return err
	}
	n.process = cmd
	return nil
}

// tenantPolicies gives each tenant one namespace per consistency class.
func tenantPolicies() string {
	policies := []string{}
	for _, tenant := range []string{"a", "b"} {
		for j, class := range []string{"strong", "causal", "eventual"} {
			policies = append(policies, fmt.Sprintf("/tenants/%s/%s/:%s:%d", tenant, class, class, j+1))
		}
	}
	return strings.Join(policies, ",")
}

func (c *LocalCluster) Kill(i int) {
	n := c.nodes[i]
	if n != nil && n.process != nil {
		_ = n.process.Process.Kill()
		_ = n.process.Wait()
		n.process = nil
	}
}

// Client connects to nodes as identity using its development certificate.
func (c *LocalCluster) Client(nodes []int, identity string) (*client.Client, error) {
	creds, err := c.credentials(identity)
	if err != nil {
		return nil, err
	}
	targets := []string{}
	for _, i := range nodes {
		targets = append(targets, fmt.Sprintf("127.0.0.1:%d", c.nodes[i].clientPort))
	}
	return client.NewWithCredentials(targets, creds)
}

func (c *LocalCluster) credentials(identity string) (credentials.TransportCredentials, error) {
	c.credsMu.Lock()
	defer c.credsMu.Unlock()
	if creds, ok := c.creds[identity]; ok {
		return creds, nil
	}
	creds, err := security.ClientCredentials(filepath.Join(c.pki, "ca.pem"), filepath.Join(c.pki, identity+".pem"), filepath.Join(c.pki, identity+".key"), "")
	if err != nil {
		return nil, err
	}
	c.creds[identity] = creds
	return creds, nil
}

// CaptureMetrics saves each node's Prometheus text as <phase>-<node>-metrics.txt.
func (c *LocalCluster) CaptureMetrics(phase string, nodes []int) error {
	for _, i := range nodes {
		if err := c.captureNodeMetrics(phase, c.nodes[i]); err != nil {
			return err
		}
	}
	return nil
}

func (c *LocalCluster) captureNodeMetrics(phase string, n *node) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://127.0.0.1:%d/metrics", n.metricsPort()), nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	_ = response.Body.Close()
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK || !strings.Contains(string(data), "meridian_raft_apply_gap") {
		return fmt.Errorf("node metrics missing")
	}
	return os.WriteFile(filepath.Join(c.dir, phase+"-"+n.id+"-metrics.txt"), data, 0600)
}

// Environment describes the cluster for environment.json.
func (c *LocalCluster) Environment() map[string]any {
	nodes := map[string]any{}
	for _, n := range c.nodes {
		nodes[n.id] = map[string]any{"peer_port": n.peerPort, "client_port": n.clientPort, "metrics_port": n.metricsPort()}
	}
	return map[string]any{"nodes": nodes}
}

// Close kills every node and closes every relay.
func (c *LocalCluster) Close() {
	for i := range c.nodes {
		c.Kill(i)
	}
	for i := range c.links {
		for _, link := range c.links[i] {
			if link != nil {
				link.close()
			}
		}
	}
}
