# Meridian — Distributed Secrets & Consistency Platform

<div align="center">

![Status](https://img.shields.io/badge/status-In%20development-orange)
![Go Version](https://img.shields.io/badge/go-1.25-blue)
![Rust Version](https://img.shields.io/badge/rust-1.87-orange)
![Python Version](https://img.shields.io/badge/python-3.12-blue)
![License](https://img.shields.io/badge/license-APACHE-green)
[![CI](https://github.com/nickemma/meridian/workflows/CI/badge.svg)](https://github.com/nickemma/meridian/actions)

**A geo-distributed key-value store and secrets management platform — built from first principles.**

_Tunable consistency per request. Strongly consistent secret writes. WASM-sandboxed policy enforcement. ML-powered access anomaly detection. Every component chaos-tested._

[Architecture](#architecture) • [Design Doc](docs/DESIGN_DOC.md) • [Runbook](docs/RUNBOOK.md) • [Tradeoffs](docs/TRADEOFFS.md) • [Roadmap](#roadmap)

</div>

---

## What is Meridian?

Meridian is two things collapsed into one, because they belong together.

At the bottom: a geo-distributed key-value store where consistency is a per-request choice, backed by a from-scratch Raft consensus engine and a Rust LSM storage layer. A client that needs a bank balance uses strong consistency. A client that needs a shopping cart uses causal consistency. A client that needs a session cache uses eventual consistency. The same cluster serves all three correctly, simultaneously.

At the top: a production-grade secrets and policy enforcement platform. Services authenticate to Meridian to fetch credentials, certificates, and API keys. Meridian enforces who can access what via a WASM-sandboxed policy engine. It rotates secrets automatically, detects access anomalies with an ML model, and writes a tamper-evident audit trail of every decision.

The reason they are one system: secrets management is a distributed storage problem. Strong consistency guarantees that a rotated credential is visible to all services before the old one is revoked. Partition tolerance guarantees that a network split does not prevent services from authenticating. Tunable consistency guarantees that a sidecar fetching a cached secret does not pay the cost of a quorum read every time. The Raft consensus engine is not a library dependency — it is the foundation. Every secret write goes through it. Every lease is backed by it. Every audit record is committed to it.

**The real question this system answers:** What happens when a service tries to fetch a database credential during a network partition? Meridian has a specific, tested, verifiable answer. Most secrets managers do not.

---

## What Each Layer Proves

| Layer | What It Demonstrates |
|---|---|
| Raft consensus from scratch | Distributed systems depth — leader election, log replication, safety properties, pre-vote extension |
| Rust LSM storage engine | Systems programming, storage internals, WAL durability, SSTable compaction, no GC on the write path |
| Tunable consistency per request | You understand when linearizability matters and when it is overhead |
| Secrets + rotation + lease management | Production platform thinking, zero-trust credential lifecycle |
| WASM-sandboxed policy engine | Security architecture, sandboxed evaluation, OPA-equivalent without the dependency |
| mTLS + node identity verification | Zero-trust networking from the cluster layer upward |
| ML access anomaly detection | Behavioral baseline, runtime enforcement, not just static policy |
| Chaos suite on secret access | The compelling demo — correctness under adversarial conditions, not just happy paths |
| Tamper-evident audit trail | You think about correctness and operability and compliance |
| Prometheus + Grafana observability | SRE ownership — the system is not built until it is observable |

---

## Architecture

![Meridian Architecture](/docs/meridian-diagram.png)

---

## The Four Pillars

### 1. Distributed Core — `core/`

The engine beneath everything. Raft consensus implemented from scratch in Go. A Rust LSM storage engine with a write-ahead log, memtable, and SSTable compaction. Tunable consistency per request — not as a configuration flag, but as a per-request protocol decision backed by the correct mechanism at each level.

- **Strong consistency** via quorum reads and writes through Raft — linearizable, verifiable
- **Causal consistency** via vector clocks — monotonic reads, causality preserved across writes
- **Eventual consistency** via async gossip replication — maximum availability, stale reads flagged explicitly
- **Pre-vote Raft extension** — partitioned nodes cannot disrupt a stable cluster on reconnect
- **Jepsen-style linearizability checker** — operation histories are mathematically verified, not assumed correct

Every secret write goes through the strong consistency path. Lease renewals go through causal. Cached credential reads go through eventual. The cluster makes this possible because the consistency model is per-request, not cluster-wide.

### 2. Secrets & Credential Management — `secrets/`

A self-hosted alternative to HashiCorp Vault, backed by your own consensus engine instead of etcd.

- **Secret storage** — API keys, database credentials, TLS certificates, service tokens, stored as strongly consistent KV entries
- **Automatic rotation** — credentials rotate on a configurable schedule or on-demand; old versions remain accessible for a grace period, then are revoked
- **Lease management** — every credential access grants a lease with a TTL; leases are tracked in the Raft log and must be renewed or they expire
- **Secret versioning** — full history of every version, who created it, when it was rotated, and what accessed each version
- **Dynamic credentials** — database credentials generated on-demand per service, not shared static passwords

The rotation event is a strongly consistent write. When a credential is rotated, the new version is committed to a quorum before the old version is marked for revocation. There is no window where both versions are simultaneously valid across a partition. This is the specific guarantee Vault-on-etcd cannot make without similar guarantees from the underlying store.

### 3. Policy Enforcement — `policy/`

An OPA-equivalent policy engine, WASM-sandboxed, embedded in every node.

- **Policy evaluation in WASM** — policies are compiled to WebAssembly and evaluated in a sandbox; a malformed or malicious policy cannot affect the node process
- **Rego-compatible policy language** — policies are written in a Rego-inspired DSL and compiled to WASM at policy upload time
- **Contextual evaluation** — policies receive the full request context: service identity (from mTLS cert), requested secret path, source IP, time of day, previous access history
- **Policy versioning and rollback** — policies are stored as strongly consistent KV entries; rollback is a strongly consistent write
- **Deny-by-default** — a service with no matching policy cannot access any secret; explicit grant is required

Every secret access is a policy decision. Policy evaluation is synchronous and on the read path — a service that cannot satisfy policy is rejected before the secret is read from storage.

### 4. Observability & Security — `observability/` + `audit/`

A system beneath everything else must be fully observable and tamper-evident.

- **Tamper-evident audit trail** — every access decision (allow or deny), every secret write, every policy change, every rotation is appended to an audit log committed through Raft; entries cannot be modified without breaking the log's hash chain
- **ML-powered access anomaly detection** — a lightweight model (Go, no external dependency) builds a behavioral baseline per service identity; accesses that deviate from baseline trigger an alert and optionally an automatic deny
- **Prometheus metrics** — replication lag, consensus latency, quorum health, secret access rate per service, policy evaluation latency, anomaly detection score per identity
- **Grafana dashboards** — cluster health, per-node write throughput, consistency level distribution, secret rotation status, lease expiry queue depth
- **Structured access logs** — every request logged with service identity, requested path, policy decision, consistency level, and latency

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| **Consensus + API + Secrets** | Go | Goroutine-per-peer Raft, gRPC server, lease management, secrets lifecycle |
| **Storage Engine** | Rust | LSM tree write performance, WAL durability, no GC on the write path |
| **Policy Engine** | Go + WASM (wasmtime) | Sandboxed evaluation, policy cannot affect node process |
| **Anomaly Detection** | Go (embedded ML) | No external dependency, behavioral baseline per service identity |
| **Chaos + Verification** | Python | Flexible orchestration, history analysis, linearizability checking |
| **Inter-node + Client API** | gRPC + Protobuf | Typed, efficient, bidirectional streaming for log replication |
| **Local Cluster** | Docker + docker-compose | Multi-node simulation, controlled network partitions via iptables |
| **Observability** | Prometheus + Grafana | Replication lag, quorum health, secret access rates, anomaly scores |

---

## Security as First Principles

- **mTLS between all cluster nodes** — no plaintext inter-node traffic; node identity is verified at every connection before any message is processed
- **Service identity via mTLS certificates** — services authenticate to Meridian with a certificate, not a password; the certificate identity is the subject of every policy decision
- **WASM-sandboxed policy evaluation** — policies run in a WebAssembly sandbox; evaluation cannot escape into the node process
- **WAL encrypted at rest** — AES-256-GCM; storage files are not plaintext on disk
- **Tamper-evident audit log** — each audit record includes a hash of the previous record; the chain cannot be silently modified
- **Node identity verification before cluster join** — a node cannot join the cluster by knowing the address; it must present a valid cluster certificate
- **Deny-by-default policy** — no implicit access; every access is an explicit allow from a matching policy
- **Lease-bound access** — every credential access is time-bounded; leaked credentials expire without manual revocation

---

## Quick Start

### Prerequisites

- Go 1.25
- Rust 1.87 (storage engine)
- Python 3.12 (chaos orchestrator + linearizability checker)
- Docker + docker-compose (multi-node cluster)

```bash
# Clone
git clone https://github.com/nickemma/meridian.git
cd meridian

# Start a 3-node local cluster
make cluster-up

# Verify cluster health
./bin/meridian-cli cluster status
# Node 1 (leader):   healthy  term=1  commit_index=0
# Node 2 (follower): healthy  term=1  commit_index=0
# Node 3 (follower): healthy  term=1  commit_index=0

# Write a secret (strong consistency — committed to quorum before returning)
./bin/meridian-cli secret put \
  --path services/payments/db-password \
  --value 'correct-horse-battery' \
  --ttl 24h

# Read a secret (strong consistency — quorum confirmed read)
./bin/meridian-cli secret get \
  --path services/payments/db-password \
  --consistency strong

# Read a cached credential (eventual — no quorum round-trip)
./bin/meridian-cli secret get \
  --path services/payments/db-password \
  --consistency eventual

# Check what policies apply to a service identity
./bin/meridian-cli policy eval \
  --identity payments-service \
  --path services/payments/db-password \
  --action read

# Rotate a secret
./bin/meridian-cli secret rotate --path services/payments/db-password

# View the audit trail for a secret path
./bin/meridian-cli audit log --path services/payments/db-password --last 20

# Run the chaos suite (isolated Docker network — destructive)
make chaos-run
```

### Consistency Levels via gRPC

```protobuf
enum ConsistencyLevel {
  STRONG   = 0;  // Quorum read/write via Raft — linearizable. Used for all secret writes.
  CAUSAL   = 1;  // Vector clock causality — monotonic reads. Used for lease renewals.
  EVENTUAL = 2;  // Async — lowest latency, stale reads flagged. Used for cached reads.
}

message SecretRequest {
  string            path             = 1;
  ConsistencyLevel  consistency      = 2;
  bytes             vector_clock     = 3;  // Required for CAUSAL reads
  string            lease_id         = 4;  // Present if renewing an existing lease
}

message SecretResponse {
  bytes   value          = 1;  // Encrypted in transit
  string  version        = 2;
  string  lease_id       = 3;
  int64   lease_ttl_sec  = 4;
  bool    stale_read     = 5;  // True if serving from eventual path and behind leader
  bytes   vector_clock   = 6;  // Return to client for subsequent causal reads
}
```

### Policy Example

```rego
# Allow payments-service to read its own DB credentials
# Deny after business hours from non-datacenter IPs

package meridian.secrets

default allow = false

allow {
  input.identity == "payments-service"
  startswith(input.path, "services/payments/")
  input.action == "read"
  is_business_hours
  is_datacenter_ip(input.source_ip)
}

is_business_hours {
  hour := time.clock(time.now_ns())[0]
  hour >= 6
  hour < 22
}

is_datacenter_ip(ip) {
  net.cidr_contains("10.0.0.0/8", ip)
}
```

### Environment Variables

```bash
MERIDIAN_NODE_ID=1
MERIDIAN_PEERS=node2:9090,node3:9090
MERIDIAN_RAFT_PORT=9090
MERIDIAN_CLIENT_PORT=8080
MERIDIAN_DATA_DIR=/var/lib/meridian
MERIDIAN_ELECTION_TIMEOUT_MS=300
MERIDIAN_HEARTBEAT_INTERVAL_MS=50
MERIDIAN_QUORUM_SIZE=2                    # for 3-node cluster
MERIDIAN_WAL_ENCRYPTION_KEY=<32-byte-key>
MERIDIAN_SECRET_ROTATION_GRACE_PERIOD=5m  # old version stays valid after rotation
MERIDIAN_LEASE_DEFAULT_TTL=1h
MERIDIAN_POLICY_WASM_MEMORY_LIMIT_MB=64
MERIDIAN_ANOMALY_BASELINE_WINDOW_HOURS=168  # 7 days of access history for baseline
MERIDIAN_AUDIT_HASH_CHAIN=true
```

---

## Engineering Deep Dive

Key system design areas implemented in Meridian:

- Raft consensus from scratch — leader election, log replication, safety properties, pre-vote extension, log compaction via snapshots
- Tunable consistency per request — strong via quorum (read index protocol), eventual via async gossip, causal via vector clocks
- Vector clock causality tracking — concurrent write detection, LWW conflict resolution, conflict log
- Partition tolerance under network splits — explicit quorum unavailability errors on strong path, stale-flagged reads on eventual path
- Rust LSM storage engine — WAL with AES-256-GCM encryption, memtable flush, SSTable compaction, bloom filters, read amplification bounds
- Secrets management — versioning, automatic rotation with grace period, lease-bound access, strongly consistent rotation events
- WASM-sandboxed policy engine — Rego-inspired DSL, sandboxed evaluation, deny-by-default, contextual policy decisions
- ML access anomaly detection — behavioral baseline per service identity, deviation scoring, optional automatic enforcement
- Tamper-evident audit trail — hash-chained records committed through Raft, end-to-end verifiable
- Chaos testing — node kills, network partitions, clock skew, secret access under partition, Jepsen-style linearizability verification

**Blog (coming soon):** _"I Merged a Distributed KV Store and a Secrets Manager Into One System. Here's Why That Makes Them Both Better."_

---

## Author

**[@nickemma](https://github.com/nickemma)** — Building production-grade distributed systems, infrastructure, and platform engineering from first principles.

💼 Open to distributed systems, infrastructure, platform, and backend engineering roles at companies building serious systems.

<div align="center">
<a href="https://www.linkedin.com/in/techieemma/"><img src="https://img.shields.io/badge/linkedin-%23f78a38.svg?style=for-the-badge&logo=linkedin&logoColor=white" alt="Linkedin"></a>
<a href="https://twitter.com/techieemma"><img src="https://img.shields.io/badge/Twitter-%23f78a38.svg?style=for-the-badge&logo=Twitter&logoColor=white" alt="Twitter"></a>
<a href="https://github.com/nickemma/"><img src="https://img.shields.io/badge/github-%23f78a38.svg?style=for-the-badge&logo=github&logoColor=white" alt="Github"></a>
<a href="https://techieemma.medium.com/"><img src="https://img.shields.io/badge/Medium-%23f78a38.svg?style=for-the-badge&logo=Medium&logoColor=white" alt="Medium"></a>
<a href="mailto:nicholasemmanuel321@gmail.com"><img src="https://img.shields.io/badge/Gmail-f78a38?style=for-the-badge&logo=gmail&logoColor=white" alt="Gmail"></a>
</div>

---

<div align="center">

**Building Systems, Building Faith — One Commit at a Time**

[⬆ Back to Top](#meridian--distributed-secrets--consistency-platform)

</div>
