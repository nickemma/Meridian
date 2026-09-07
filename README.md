# Meridian — Distributed Secrets & Consistency Platform

<div align="center">

![Status](https://img.shields.io/badge/status-v1%20complete-brightgreen)
![Tests](https://img.shields.io/badge/tests-Go%20%2B%20Rust%20passing-brightgreen)
![Go Version](https://img.shields.io/badge/go-1.26-blue)
![Rust Version](https://img.shields.io/badge/rust-1.87-orange)
![Python Version](https://img.shields.io/badge/python-3.12-blue)
![License](https://img.shields.io/badge/license-APACHE-green)
[![CI](https://github.com/nickemma/meridian/workflows/CI/badge.svg)](https://github.com/nickemma/meridian/actions)

**A geo-distributed key-value store and secrets management platform — built from first principles.**

_Raft consensus and an LSM storage engine written from scratch. Versioned secrets with lease-bound access and safe rotation. Deny-by-default policy on every access. A hash-chained audit trail. Chaos-tested on a live 3-node cluster._

[Architecture](#architecture) • [Design Doc](docs/DESIGN_DOC.md) • [Runbook](docs/RUNBOOK.md) • [Tradeoffs](docs/TRADEOFFS.md) • [Roadmap](#roadmap)

</div>

---

## What is Meridian?

Meridian is two things collapsed into one, because they belong together.

At the bottom: a geo-distributed key-value store where consistency is a per-request choice, backed by a from-scratch Raft consensus engine and a Rust LSM storage layer. A client that needs a bank balance uses strong consistency. A client that needs a shopping cart uses causal consistency. A client that needs a session cache uses eventual consistency. The same cluster serves all three correctly, simultaneously.

At the top: a secrets and policy enforcement platform. Services authenticate to Meridian to fetch credentials, certificates, and API keys. Meridian enforces who can access what via a sandboxed policy engine. It rotates secrets automatically, scores access against a per-identity behavioral baseline, and writes a tamper-evident audit trail of every decision.

The reason they are one system: secrets management is a distributed storage problem. Strong consistency guarantees that a rotated credential is visible to all services before the old one is revoked. Partition tolerance guarantees that a network split does not prevent services from authenticating. Tunable consistency guarantees that a sidecar fetching a cached secret does not pay the cost of a quorum read every time. The Raft consensus engine is not a library dependency — it is the foundation. Every secret write goes through it. Every lease is backed by it. Every audit record is committed to it.

**The real question this system answers:** What happens when a service tries to fetch a database credential during a network partition? Meridian has a specific answer, and it is the reason the consensus engine is written from scratch rather than imported. Most secrets managers do not have one.

**Where this stands:** v1 is complete — the consensus engine, the storage engine, the secrets and policy layers, the audit trail, the metrics, and the chaos suite are all built and tested. [Project Status](#project-status) is the row-by-row account, including what is designed but not yet built. Read it before you read anything else here as a claim.

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
| Access anomaly detection | Behavioral baseline per identity, explainable thresholds, not just static policy |
| Chaos suite on secret access | The compelling demo — correctness under adversarial conditions, not just happy paths |
| Tamper-evident audit trail | You think about correctness and operability and compliance |
| Prometheus + Grafana observability | SRE ownership — the system is not built until it is observable |

---

## Architecture

![Meridian Architecture](/docs/meridian-diagram.png)

---

## The Four Pillars

These four sections describe the system as designed. [Project Status](#project-status) says
which parts of each are built today.

### 1. Distributed Core — `internal/raft/` + `storage-engine/`

The engine beneath everything. Raft consensus implemented from scratch in Go. A Rust LSM storage engine with a write-ahead log, memtable, and SSTable compaction. Tunable consistency per request — not as a configuration flag, but as a per-request protocol decision backed by the correct mechanism at each level.

- **Strong consistency** via quorum reads and writes through Raft — linearizable, verifiable
- **Causal consistency** via vector clocks — monotonic reads, causality preserved across writes
- **Eventual consistency** via async gossip replication — maximum availability, stale reads flagged explicitly
- **Pre-vote Raft extension** — partitioned nodes cannot disrupt a stable cluster on reconnect
- **Jepsen-style linearizability checker** — operation histories are mathematically verified, not assumed correct

Every secret write goes through the strong consistency path. Lease renewals go through causal. Cached credential reads go through eventual. The cluster makes this possible because the consistency model is per-request, not cluster-wide.

### 2. Secrets & Credential Management — `internal/secrets/`

A self-hosted alternative to HashiCorp Vault, backed by your own consensus engine instead of etcd.

- **Secret storage** — API keys, database credentials, TLS certificates, service tokens, stored as strongly consistent KV entries
- **Automatic rotation** — credentials rotate on a configurable schedule or on-demand; old versions remain accessible for a grace period, then are revoked
- **Lease management** — every credential access grants a lease with a TTL; leases are tracked in the Raft log and must be renewed or they expire
- **Secret versioning** — full history of every version, who created it, when it was rotated, and what accessed each version
- **Dynamic credentials** — database credentials generated on-demand per service, not shared static passwords

The rotation event is a strongly consistent write. When a credential is rotated, the new
version is committed to a quorum *before* the old version is scheduled for revocation, and
the old version remains valid for a configurable grace period so that clients holding it are
never broken by someone else's rotation.

The guarantee is therefore the inverse of the obvious one, and stronger: **there is no window
in which neither version is valid.** A client that fetched a credential a second before a
rotation, on a node that has not yet seen the new version, still holds something that works.
Both-valid is a bounded, deliberate overlap; neither-valid is an outage, and the ordering of
the commit is what rules it out.

### 3. Policy Enforcement — `internal/policy/`

An OPA-equivalent policy engine, WASM-sandboxed, embedded in every node.

- **Policy evaluation in WASM** — policies are compiled to WebAssembly and evaluated in a sandbox; a malformed or malicious policy cannot affect the node process
- **Rego-compatible policy language** — policies are written in a Rego-inspired DSL and compiled to WASM at policy upload time
- **Contextual evaluation** — policies receive the full request context: service identity (from mTLS cert), requested secret path, source IP, time of day, previous access history
- **Policy versioning and rollback** — policies are stored as strongly consistent KV entries; rollback is a strongly consistent write
- **Deny-by-default** — a service with no matching policy cannot access any secret; explicit grant is required

Every secret access is a policy decision. Policy evaluation is synchronous and on the read path — a service that cannot satisfy policy is rejected before the secret is read from storage.

### 4. Observability & Security — `internal/metrics/` + `internal/audit/` + `internal/anomaly/`

A system beneath everything else must be fully observable and tamper-evident.

- **Tamper-evident audit trail** — every access decision (allow or deny), every secret write, every policy change, every rotation is appended to an audit log committed through Raft; entries cannot be modified without breaking the log's hash chain
- **Access anomaly detection** — a per-identity behavioral baseline over four features: request
  rate, the set of source IPs, the set of secret paths, and the denial ratio. Each is scored by
  a rule with an explicit threshold: more than 100 requests/minute over a 5-minute sliding
  window (severity scaled by how far over), a source IP never seen before, a secret path never
  seen before, or a denial rate above 50%. No identity is scored until it has 10 observations,
  and the IP and path rules stay silent until the baseline has seen 3 IPs and 5 paths — a
  detector that fires on its first sighting of everything is a detector nobody leaves on.
  Above threshold: an alert on a channel, with the reason attached in words.

  The method is deliberately simple, and calling it ML would be generous. Four thresholds and a
  sliding window are defensible in a review, and every alert explains itself in one sentence —
  `request rate 143.0 req/min exceeds threshold 100.0`. A model nobody can explain is a
  liability in the one conversation where it matters. Auto-deny on an alert is the design's
  intent and is not wired in yet; quarantine must be reversible and audited before it is.
- **Prometheus metrics** — replication lag, consensus latency, quorum health, secret access rate per service, policy evaluation latency, anomaly detection score per identity
- **Grafana dashboards** — cluster health, per-node write throughput, consistency level distribution, secret rotation status, lease expiry queue depth
- **Structured access logs** — every request logged with service identity, requested path, policy decision, consistency level, and latency

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| **Consensus + API + Secrets** | Go | Goroutine-per-peer Raft, gRPC server, lease management, secrets lifecycle |
| **Storage Engine** | Rust | LSM tree write performance, WAL durability, no GC on the write path |
| **Policy Engine** | Go — WASM (wasmtime) in v2 | Sandboxed evaluation, policy cannot affect node process |
| **Anomaly Detection** | Go, no dependencies | Statistical baseline per service identity, in-process, explainable |
| **Chaos + Verification** | Python | Flexible orchestration, history analysis, linearizability checking |
| **Inter-node + Client API** | gRPC + Protobuf | Typed, efficient, bidirectional streaming for log replication |
| **Local Cluster** | Docker + docker-compose | Multi-node simulation, controlled network partitions via iptables |
| **Observability** | Prometheus + Grafana | Replication lag, quorum health, secret access rates, anomaly scores |

---

## Security as First Principles

The security model, and where each principle stands. **Do not run this on a network you do not
control** — the transport work is v2, and this section is explicit about that rather than
quiet.

- ✅ **Deny-by-default policy** — no implicit access; every access is an explicit allow from a matching policy, and the rejecting rule is recorded as the reason
- ✅ **Lease-bound access** — every credential access is time-bounded; leaked credentials expire without manual revocation
- ✅ **Tamper-evident audit log** — each audit record includes a hash of the previous record; the chain cannot be silently modified, and the verifier walks it end to end
- ✅ **Rotation with a grace period** — the new version is committed before the old one is scheduled for revocation; no window in which neither is valid
- ⬜ **mTLS between all cluster nodes** — inter-node gRPC is plaintext today. This is the largest single gap between the design and the code
- ⬜ **Service identity via mTLS certificates** — the policy engine already treats identity as the subject of every decision; today that identity is asserted by the caller, not proven by a certificate
- ⬜ **WASM-sandboxed policy evaluation** — evaluation is native Go behind the interface the sandbox will sit on
- ⬜ **WAL encrypted at rest** — the WAL is CRC32-checksummed for integrity, not encrypted for confidentiality
- ⬜ **Node identity verification before cluster join** — a node currently joins by knowing the address

---

## Quick Start

### Prerequisites

- Go 1.26
- Rust 1.87 (storage engine)
- Python 3.12 (chaos orchestrator + linearizability checker)
- Docker + docker-compose (multi-node cluster)

```bash
# Clone
git clone https://github.com/nickemma/meridian.git
cd meridian

# Build the node binary
make build

# Run every Go test (Raft, secrets, policy, audit, anomaly, metrics)
make test

# Run the Rust storage engine tests (WAL, memtable, SSTable, crash recovery)
cd storage-engine && cargo test && cd ..

# Start a 3-node local cluster — plus Prometheus and Grafana
make cluster-up

# Watch the cluster elect a leader
make cluster-logs
# [raft] node node-1 starting — role=follower term=0
# [raft] node-1 starting election for term 1
# [raft] node-1 received vote from node-2 (total: 2)
# [raft] node-1 won election for term 1 with 2 votes
# [raft] node-1 starting heartbeat ticker

# Scrape a node's metrics directly (each node serves them on client port + 1000,
# published to the host as 9081 / 9082 / 9083)
curl -s localhost:9081/metrics | grep meridian_raft

# Prometheus and Grafana
open http://localhost:9099   # Prometheus
open http://localhost:3000   # Grafana — admin / meridian

# Run the chaos suite — kills nodes, partitions the network, skews clocks
# (isolated Docker network — destructive; brings the cluster up and down itself)
make chaos-run

# Tear down
make cluster-down
```

> **On the CLI.** The `meridian-cli` admin binary is v2 scope. Until the secrets and policy
> packages are mounted on the client gRPC path there is nothing for it to call, so it is not
> shipped rather than shipped broken. The sections that follow describe the interfaces those
> packages expose today, in the shape the CLI and the client API will use.

### Secret and Policy Interfaces

The secrets store is command-driven — every mutation is a `Command` value, which is what makes
it drop-in ready to be applied from a committed Raft log entry rather than called directly.

```go
// Write a secret
cmd, _ := secrets.NewPutCommand("services/payments/db-password",
    []byte("correct-horse-battery"), "payments-service")
store.Apply(cmd)

// Rotate it — the new version is written first; the old one is stamped
// with RevokeAt = now + grace, so there is never a neither-valid window.
cmd, _ = secrets.NewRotateCommand("services/payments/db-password",
    []byte("new-value"), "rotation-controller", 5*time.Minute)
store.Apply(cmd)

// Read the current version, and the full version history
secret, err := store.Get("services/payments/db-password")
versions, err := store.ListVersions("services/payments/db-password")

// Every access is a policy decision — deny-by-default
decision := engine.Evaluate(&policy.Request{
    Identity:  "payments-service",
    Path:      "services/payments/db-password",
    Action:    policy.ActionRead,
    SourceIP:  "10.0.1.7",
    Timestamp: time.Now(),
})
// decision.Allowed, decision.Reason, decision.Policy — the reason is logged either way

// Every decision lands in the hash-chained audit log
auditLog.Append(audit.EventSecretRead, "payments-service",
    "services/payments/db-password", "success",
    map[string]string{"version": "3"}, traceID)
auditLog.Verify() // walks the chain — non-nil error means a record was modified
```

### Consistency Levels via gRPC

The client API below is the v2 target. What the node serves today is `RaftService`
(`RequestVote`, `AppendEntries`, `PreVote`) in `proto/raft/raft.proto`.

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

Policies are JSON rule sets today. Every rule in a policy must match for the policy to allow;
if no policy matches, the request is denied — there is no implicit grant.

```json
{
  "name": "payments-db-read",
  "version": 1,
  "rules": [
    {
      "identities": ["payments-service"],
      "path_prefix": "services/payments/",
      "actions": ["read"],
      "allowed_cidrs": ["10.0.0.0/8"],
      "business_hours_only": true
    }
  ]
}
```

The same policy, evaluated: `payments-service` may read anything under `services/payments/`,
only from the datacenter range, only between 06:00 and 22:00 UTC. Every other request — wrong
identity, wrong path, wrong action, an office IP, 03:00 — is denied with the specific rule that
rejected it recorded as the reason.

The Rego-inspired DSL and its WASM compiler are the v2 shape of this. The rule set above is
what the engine actually evaluates today; see the [Project Status](#project-status) table.

### Environment Variables

Read by the node at startup. The first four are required — a missing one is a startup failure
with a named error, never a silent default.

```bash
MERIDIAN_NODE_ID=node-1                   # required
MERIDIAN_RAFT_PORT=9090                   # required — peer-to-peer Raft RPCs
MERIDIAN_CLIENT_PORT=8080                 # required — client API; metrics on this + 1000
MERIDIAN_DATA_DIR=/var/lib/meridian       # required — WAL and SSTables
MERIDIAN_PEERS=node-2:9090,node-3:9090    # optional — empty means a single-node cluster;
                                          # quorum size is derived: (peers+1)/2 + 1
```

Election timeout (150–300 ms, randomised), heartbeat interval (50 ms), rotation grace period,
lease TTL, and the anomaly thresholds are compile-time defaults today, not environment
variables. They are constructor arguments in `internal/config` and `internal/anomaly` —
promoting them to env vars is v2 work, and this list will grow when it happens.

---

## Project Status

> **Meridian is a working system with components at different levels of maturity. This table
> says which is which.**
> Everything below the table is documented at full design detail regardless of state — that is
> deliberate, and this table is how you tell the two apart. Nothing in this README is claimed
> as shipped unless it says so here.

**v1 is complete.** The six phases that were scoped for v1 — cluster skeleton, Rust storage
engine, Raft consensus, secrets and policy, audit and observability, chaos suite — are all
built and tested. `go test ./...` and `cargo test` pass; the chaos suite exercises node kill,
minority partition, and clock skew against a live 3-node cluster. The rows below marked
*designed, not built* are v2 scope, not unfinished v1 work.

| Component | State |
|---|---|
| Raft consensus — election, replication, safety | **Working** — state machine, `RequestVote`, `AppendEntries`, commit index, unit-tested |
| Raft — pre-vote extension | **Working** — a partitioned node cannot raise the term on reconnect |
| Raft — log compaction / snapshots | **Designed, not built** — the log grows unbounded today |
| Rust LSM storage engine — WAL, memtable, SSTable | **Working** — CRC32-checked WAL, crash recovery, memtable flush, tombstones; 22 tests |
| LSM — compaction, bloom filters | **Partial** — SSTable merge iterator is in place; the compaction scheduler and bloom filters are not |
| WAL encryption at rest (AES-256-GCM) | **Designed, not built** — the WAL is checksummed, not encrypted |
| Tunable consistency — strong path (quorum + read index) | **Partial** — quorum commit through Raft works; the read-index protocol and the client read API are not built |
| Tunable consistency — causal path (vector clocks) | **Designed, not built** |
| Tunable consistency — eventual path (gossip) | **Designed, not built** |
| Secret storage, versioning, lease management | **Working, in-process** — versioned store, lease TTL and renewal, unit-tested; driven directly, not yet through the Raft log |
| Automatic rotation with grace period | **Working, in-process** — new version committed first, old version carries a `RevokeAt` after the grace period |
| Dynamic per-service credentials | **Designed, not built** |
| WASM policy engine — sandbox, evaluation | **Partial** — deny-by-default evaluation works behind the sandbox interface; the implementation is native Go, not yet WASM |
| Policy DSL → WASM compiler | **Designed, not built** — policies are JSON rule sets today |
| mTLS between nodes, identity verification on join | **Designed, not built** — inter-node gRPC is plaintext |
| Tamper-evident audit trail (hash chain through Raft) | **Partial** — the SHA-256 chain appends and verifies end to end; records are not yet committed through Raft |
| Access anomaly detection | **Working** — per-identity baselines with four deviation rules; statistical, not a learned model |
| Linearizability checker | **Working, unexercised** — simplified Wing-Gong checker, unit-shaped and standalone; nothing records histories for it until the client API exists |
| Chaos suite | **Working** — node kill, minority partition, clock skew; each scenario asserts the cluster reforms afterwards |
| Prometheus metrics + Grafana dashboards | **Partial** — 17 metrics exported and scraped, Grafana wired into compose; no dashboards committed to the repo yet |

Vocabulary, so the column is comparable across repositories:
**Working** · **Working, in-process** (built and tested, not yet on the request path) ·
**Partial** (what's missing, in one clause) · **Skeleton** · **Designed, not built**

**One integration caveat, stated plainly:** the node binary that runs in the cluster today
serves the Raft RPCs and the metrics endpoint. The secrets store, policy engine, audit log,
and anomaly detector are complete, tested packages that are not yet mounted on that gRPC
path — wiring them behind the client API is the first task of v2, along with the CLI.

---

## Service Level Objectives

The platform is measured the way a real production dependency should be. The targets are
design targets. The **Verified** column says how each one is checked today — and where it says
*not yet measured*, no number is claimed, because a target with an invented measurement beside
it is worse than an empty cell.

| SLI | Definition | Target | Verified |
|---|---|---|---|
| **Strong-path read latency** | Quorum-confirmed secret read, p99 | design target, unset | Not yet measured — no benchmark harness |
| **Strong-path write latency** | Raft commit to quorum, p99 | design target, unset | Not yet measured — no benchmark harness |
| **Eventual-path read latency** | Local read, p99 | design target, unset | Path not built |
| **Linearizability on the strong path** | Histories violating linearizability | **0** | Checker built and standalone — no histories to feed it until the client API exists |
| **Rotation safety** | Requests failing during a rotation | **0** | Unit-tested in `internal/secrets` — not yet under chaos |
| **Revocation propagation** | Revocation → enforced cluster-wide, p99 | design target, unset | Not yet measured |
| **Availability under one node loss** | Strong-path requests served, 3-node cluster | 100% | Chaos suite — node kill: cluster survives, killed node rejoins |
| **Behavior under quorum loss** | Strong-path requests incorrectly served | **0 — fail closed** | Chaos suite — minority partition: the isolated node cannot win an election. 2-of-3 kill is not in the battery yet |
| **Policy evaluation latency** | Added per access decision, p99 | < 2 ms | Histogram bucketed for it; not yet measured on a live path |
| **Audit chain integrity** | Verifier runs detecting an unexplained break | **0** | `Log.Verify()` unit-tested, including deliberate tampering |

The zeros are invariants, not percentiles. An invariant with an error budget is not an
invariant. `fail closed` under quorum loss is the row that defines this system's character:
a secrets manager that serves reads it cannot confirm has stopped being a secrets manager.

The empty latency cells are the honest state of v1: correctness is verified, performance is
not yet characterised. Benchmarks are the first thing v2 owes this table.

---

## Metrics

Every metric is a deliberate answer to a question someone will ask at 3 a.m. These 17 are
exported today, every one carrying a `node` label, scraped by the Prometheus in
`infra/docker-compose.yml`.

| Metric | Type | Labels | Question it answers |
|---|---|---|---|
| `meridian_raft_current_term` | gauge | node | Are the nodes in the same term, or is one adrift? |
| `meridian_raft_role` | gauge | node | Who is leader — and is exactly one node claiming it? |
| `meridian_raft_commit_index` | gauge | node | Which follower is behind, and by how much? |
| `meridian_raft_log_entries_total` | gauge | node | Is the log growing without bound? (It is — compaction is v2.) |
| `meridian_raft_elections_total` | counter | node | Is the cluster stable, or flapping? |
| `meridian_raft_heartbeats_total` | counter | node | Is the leader alive and is the wire actually carrying traffic? |
| `meridian_secrets_reads_total` | counter | node, outcome | Who is reading, and how often does it fail? |
| `meridian_secrets_writes_total` | counter | node, outcome | Same question, on the path that costs a quorum |
| `meridian_secrets_rotations_total` | counter | node | Did the rotation controller actually run? |
| `meridian_secrets_active_leases` | gauge | node | How much access is outstanding right now? |
| `meridian_secret_operation_duration_seconds` | histogram | node, operation | Which operation is the slow one? |
| `meridian_policy_evaluations_total` | counter | node, decision | Is a policy wrong, or is someone probing? |
| `meridian_policy_evaluation_duration_seconds` | histogram | node | Is policy on the read path costing what we said? |
| `meridian_storage_wal_writes_total` | counter | node | Is the write path reaching disk at the rate we think? |
| `meridian_storage_memtable_size_bytes` | gauge | node | How close is the next flush? |
| `meridian_storage_sstable_count` | gauge | node | Is read amplification climbing? |
| `meridian_audit_records_total` | counter | node | Is every decision leaving a trace, or are some silent? |

`meridian_policy_evaluation_duration_seconds` uses tight buckets — 0.1 ms to 10 ms — because
policy sits on the read path and the only interesting question about it is whether it is cheap.
Default buckets would have hidden the entire distribution in the first one.

Quorum availability, stale reads, rotation grace, and anomaly scores are metrics the
[Failure Mode Analysis](#failure-mode-analysis) below leans on. They land when the components
they measure are on the request path — the same v2 wiring as the client API.

---

## Failure Mode Analysis

The analysis the design is built against. Node death, minority partition, partition healing,
and clock skew are exercised by the chaos suite today; the rest name detections and mitigations
that arrive with the components they belong to — the metric names in this table are the target
vocabulary, and not all of them are exported yet.

| Failure | Blast radius | Detection | Mitigation |
|---|---|---|---|
| Leader dies | Writes pause for one election | `raft_leader_changes_total` | Election completes in `ELECTION_TIMEOUT_MS`; followers serve eventual reads throughout |
| Quorum lost (2 of 3 down) | All strong-path requests | `quorum_available` = 0 | **Fail closed** on strong path; eventual path serves with `stale_read=true` |
| Network partition, minority side | Clients on that side | Quorum failure rate | Minority refuses strong reads and writes; pre-vote prevents it disrupting the majority on reconnect |
| Partitioned node rejoins | Would be a spurious election | Term increase without leadership loss | Pre-vote: the rejoining node cannot raise the term without winning a pre-election first |
| Secret fetched during a partition | One service's startup | Strong-path error, explicit | Explicit quorum-unavailable error, never a silently stale credential. **This is the system's headline guarantee** |
| Rotation interrupted mid-flight | One secret path | `rotation_grace_active` stuck | New version is committed before the old is scheduled for revocation; an interrupted rotation leaves the old version valid, never neither |
| Lease expires during a long operation | One service | `lease_expirations_total` with `renewed=false` | Renewal is on the causal path, cheap enough to do often; expiry is a client bug with a metric |
| Policy engine rejects a valid request | One identity | `policy_denials_total` by reason | `policy eval` explains the decision; deny-by-default means a missing policy looks identical to a hostile one, which is why the reason is recorded |
| Malicious or malformed policy | Would be the node process | Sandbox violation counter | WASM sandbox with a memory limit; evaluation cannot escape |
| Audit chain break | Trust in the entire record | Verifier run | Alert, freeze writes, reconcile. **Tamper-evident, not tamper-proof** — the chain proves modification, it does not prevent it |
| WAL corruption on one node | That node's data | Checksum failure on replay | Node refuses to start; rebuilt from a peer's snapshot rather than repaired in place |
| Clock skew across nodes | Lease TTL correctness | NTP drift metric | TTLs evaluated against a monotonic source; Raft never depends on wall time for safety |
| Anomaly false positive | One identity, wrongly denied | Reversal rate | Auto-deny is opt-in per identity; quarantine reversible and audited |

Two rows fail closed. That is the design's stance and its cost: Meridian will refuse to serve
rather than serve something it cannot confirm. A secrets manager that prefers availability has
chosen not to be a secrets manager.

---

## Roadmap

### v1 — shipped

**Agree.** ✅ Raft from scratch · leader election · log replication · safety properties · pre-vote — ⬜ snapshots

**Store.** ✅ Rust LSM · WAL with crash recovery · memtable · SSTable with tombstones — ⬜ compaction scheduler · bloom filters · encryption at rest

**Secure.** ✅ Secret storage and versioning · leases with TTL · automatic rotation with grace — ⬜ dynamic per-service credentials · mTLS and node identity verification

**Decide.** ✅ Deny-by-default policy evaluation · contextual rules — ⬜ WASM sandbox · policy DSL and compiler · versioning and rollback

**Watch.** ✅ Tamper-evident audit log · access anomaly detection · Prometheus metrics · chaos suite — ⬜ audit committed through Raft · Grafana dashboards in-repo

**Choose.** ⬜ Deferred to v2 in full — the strong path is quorum commit through Raft today; causal and eventual are designed, not built

### v2 — next, in order

1. **Wire it up.** Client gRPC API — mount secrets, policy, audit, and the anomaly detector on the request path, backed by committed Raft entries rather than direct calls. Everything below depends on this.
2. **`meridian-cli`.** Cluster status, secret CRUD, policy upload and eval, audit query.
3. **Benchmarks.** Fill the empty cells in the SLO table; nothing about performance is claimed until they exist.
4. **Snapshots and log compaction.** The Raft log grows unbounded today.
5. **The consistency menu.** Read-index for strong reads, vector clocks for causal, gossip for eventual — with the linearizability checker finally fed real histories.
6. **The security work as designed.** mTLS between nodes, WAL encryption at rest, and moving policy evaluation into an actual WASM sandbox.

**Deferred, deliberately, past v2:** multi-region replication · HSM-backed root keys · secret sharing across clusters · PKI issuance as a first-class secret type · Kubernetes operator

---

## Non-Goals

- **Not a general-purpose database.** Meridian is a KV store shaped by what secrets management needs. Its consistency options exist for credential workloads, not for yours.
- **Not a drop-in Vault replacement.** No Vault API compatibility, no plugin ecosystem, no enterprise support. It is the layer Vault delegates, built rather than configured.
- **Not tamper-proof.** Tamper-*evident*. Hash chaining proves modification; it does not prevent an operator with write access from attempting it.
- **Not highly available under quorum loss.** The strong path fails closed, deliberately. If that is unacceptable for your workload, you want a cache, not a secrets manager.
- **Not a certificate authority.** Meridian stores and rotates certificates. Issuing them is [SYNAPSE-AI](https://github.com/nickemma/synapse-ai)'s problem.

---

## Engineering Deep Dive

The system design areas Meridian is built around. Marked ✅ where v1 built it, ⬜ where the
[Project Status](#project-status) table says it is design work — the list is the same either
way, because the design is what the code is being built toward.

- ✅ Raft consensus from scratch — leader election, log replication, safety properties, pre-vote extension
- ✅ Rust LSM storage engine — write-ahead log with CRC32 integrity, crash recovery from the WAL, memtable flush, SSTable with tombstones
- ✅ Secrets management — versioning, automatic rotation with grace period, lease-bound access, command-shaped mutations ready for the Raft log
- ✅ Policy engine — deny-by-default, contextual decisions on identity, path, action, source CIDR, and time of day, with the rejecting rule as the reason
- ✅ Access anomaly detection — per-identity behavioral baseline, four deviation rules with explicit thresholds and a learning period
- ✅ Tamper-evident audit trail — SHA-256 hash-chained records, end-to-end verifiable, tampering detected by re-walking the chain
- ✅ Chaos testing — node kills, network partitions, clock skew, cluster recovery assertions
- ⬜ Partition tolerance semantics — explicit quorum-unavailability errors on the strong path, stale-flagged reads on the eventual path
- ⬜ Tunable consistency per request — read-index for strong, vector clocks for causal, async gossip for eventual
- ⬜ Vector clock causality tracking — concurrent write detection, LWW conflict resolution, conflict log
- ⬜ WASM sandboxing and the Rego-inspired DSL — the evaluation model is built; the sandbox and the compiler are not
- ⬜ Storage hardening — compaction scheduler, bloom filters, AES-256-GCM encryption at rest
- ⬜ Jepsen-style linearizability verification against live histories — the checker exists and waits on the client API

**Blog (coming soon):** _"I Merged a Distributed KV Store and a Secrets Manager Into One System. Here's Why That Makes Them Both Better."_

---

## Layout

```
cmd/meridian/            node binary — config, metrics server, gRPC server, Raft node
cmd/meridian-cli/        admin CLI — v2
internal/raft/           consensus: state machine, election, pre-vote, replication, commit, timer, peers
internal/secrets/        versioned secret store, commands, leases, rotation with grace
internal/policy/         policy model and deny-by-default evaluation engine
internal/audit/          hash-chained, append-only audit log with a verifier
internal/anomaly/        per-identity access baselines and deviation rules
internal/metrics/        Prometheus collectors and the /metrics server
internal/config/         env-var config, loaded once at startup, never mutated
internal/server/         gRPC server wiring
storage-engine/src/      Rust LSM: wal.rs, memtable.rs, sstable.rs, engine.rs
proto/raft/              RaftService — RequestVote, AppendEntries, PreVote
chaos/                   orchestrator, scenarios, linearizability verifier
infra/                   docker-compose (3 nodes + Prometheus + Grafana), prometheus.yml
docs/                    design doc, architecture, runbook, tradeoffs, roadmap
```

`internal/raft` imports nothing from the layers above it — the safety properties are
arithmetic over terms and indexes, and they are tested without a network. Every package above
it depends downward only, which is why the secrets store takes a `Command` rather than calling
Raft: applying a committed entry and applying a direct call are the same code path.

---

## One Platform, Six Repositories

These are not six projects. **EMBER** fronts everything · **LATTICE** proves the distributed
core · **MERIDIAN** provides secrets, policy, lease and audit · **VEYRONIX** consumes MERIDIAN
and operates services · **TESSERA** is served behind EMBER and operated like VEYRONIX ·
**SYNAPSE-AI** governs TESSERA's agents using MERIDIAN's lineage and EMBER's data plane.

Meridian is the seed. Its policy engine, lease semantics, and audit lineage are what
SYNAPSE-AI extends with AI context; its secret injection is what VEYRONIX consumes at deploy
time.

[EMBER](https://github.com/nickemma/ember) · [LATTICE](https://github.com/nickemma/lattice) ·
[MERIDIAN](https://github.com/nickemma/meridian) · [VEYRONIX](https://github.com/nickemma/veyronix) ·
[TESSERA](https://github.com/nickemma/tessera) · [SYNAPSE-AI](https://github.com/nickemma/synapse-ai)

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
