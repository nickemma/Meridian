# Meridian Architecture

**Status:** In Development
**Last Updated:** April 2026
**Author:** [@nickemma](https://github.com/nickemma)

---

## Overview

Meridian is a geo-distributed key-value store and secrets management platform built under a single architectural constraint: **every guarantee the consensus engine provides must translate directly into a guarantee the secrets layer can make to its clients.**

Strong consistency via Raft means that a rotated credential is visible to all nodes in the majority partition before the old version is revoked — there is no window where two versions are simultaneously valid across a split. Partition tolerance means that a network partition produces an explicit, typed error on the strong path and an explicitly-flagged stale read on the eventual path — not a silent wrong answer. Tunable consistency means that a sidecar agent doing a cached credential read does not pay the cost of a quorum round-trip every time, because the consistency model is a per-request decision, not a cluster-wide setting.

The central design constraint: **each layer must be implemented correctly, or the system is not useful — it is deceptive.** A secrets manager that claims strong consistency but allows stale credential reads under partition is worse than one that makes no consistency guarantee at all. Every component in Meridian is built under the constraint that its guarantees are mathematically verifiable by the chaos suite.

---

## System Map

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Client Layer                                 │
│   gRPC API  •  Secret fetch  •  Policy check  •  Lease renewal      │
│   Consistency level per request: Strong | Causal | Eventual         │
└─────────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────────┐
│                      Meridian Node (Go)                              │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────────────┐ │
│  │   Request    │  │  Consistency │  │     Vector Clock          │ │
│  │   Router     │  │  Resolver    │  │     Manager               │ │
│  └──────────────┘  └──────────────┘  └───────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────────┐
│                    Secrets & Policy Layer (Go)                       │
│  ┌───────────────┐  ┌───────────────┐  ┌──────────────────────┐   │
│  │  Secret Store │  │ Policy Engine │  │  Anomaly Detector    │   │
│  │  + Rotation   │  │ (WASM sandbox)│  │  (ML model, Go)      │   │
│  └───────────────┘  └───────────────┘  └──────────────────────┘   │
│  ┌───────────────┐  ┌───────────────┐                              │
│  │  Lease Manager│  │  Audit Engine │                              │
│  │               │  │  (append-only)│                              │
│  └───────────────┘  └───────────────┘                              │
└─────────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────────┐
│                    Raft Consensus Layer (Go)                          │
│   Leader Election  •  Log Replication  •  Commit  •  Snapshot        │
└─────────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────────┐
│   ┌───────────────────────┐   ┌──────────────────────────────┐     │
│   │    Storage Engine     │   │      Replication Layer       │     │
│   │       (Rust)          │   │          (Go)                │     │
│   │  LSM  •  WAL  •  SSTables │   │  Async gossip            │    │
│   └───────────────────────┘   └──────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────────┐
│                       Cluster (3–5 nodes)                            │
│   Node 1 (Leader)  •  Node 2 (Follower)  •  Node 3 (Follower)      │
│                  mTLS between all nodes                              │
└─────────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────────┐
│               Chaos Orchestrator + Verifier (Python)                 │
│   Node kills  •  Partitions  •  Clock skew  •  Linearizability      │
│   Secret access under partition  •  Policy enforcement under chaos   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Component Responsibilities

### Request Router (Go)

The entry point for all client requests — both raw KV operations and secrets API calls. Inspects the requested consistency level and routes accordingly:

- `STRONG` → Raft consensus layer (all writes and reads go through the leader)
- `CAUSAL` → Vector clock manager + storage (reads return the value at or after the client's vector clock)
- `EVENTUAL` → Local storage read (any node serves directly from its local state)

For secrets API requests, the router additionally:
- Extracts the service identity from the mTLS certificate presented by the client
- Invokes the policy engine before any storage read or write — a request that fails policy evaluation never reaches the storage layer
- Records every decision in the audit engine, regardless of outcome

The router is stateless. It does not make consistency decisions or policy decisions — it delegates to the component responsible for each concern.

### Secret Store (Go)

Sits above the consensus layer. Every secret is stored as a structured KV entry with a versioned path:

```
Key:   secret:<path>:<version>
Value: {ciphertext, created_at, created_by, ttl, rotation_schedule, metadata}

Key:   secret:<path>:current
Value: {version: "v4", valid_until: <timestamp>}
```

The `current` pointer is a strongly consistent write. Rotation atomically updates the `current` pointer to the new version and schedules the old version for revocation after the grace period. Both writes — the new version entry and the `current` pointer update — go through Raft in a single log entry. There is no two-step window where the pointer update can be separated from the version creation by a crash.

Secret values are encrypted with a per-secret DEK (data encryption key). The DEK is wrapped with a cluster-level KEK (key encryption key) stored in a sealed keyring. The plaintext DEK never touches disk.

### Policy Engine (Go + WASM)

Evaluates access policy for every secret request. Policies are written in a Rego-inspired DSL, compiled to WebAssembly at upload time, and evaluated in a wasmtime sandbox on the read path.

The WASM sandbox is the critical design decision. A policy that panics, loops indefinitely, or attempts to access memory outside its sandbox cannot affect the node process. The evaluation timeout is enforced at the WASM fuel level — not by a goroutine timer that can be bypassed. A policy that exceeds its fuel budget is terminated and the request is denied.

Every policy evaluation receives the full request context:
```json
{
  "identity":        "payments-service",
  "path":            "services/payments/db-password",
  "action":          "read",
  "source_ip":       "10.4.2.31",
  "timestamp":       1712345678,
  "lease_id":        "lease-abc-123",
  "access_history":  { "last_access": 1712342000, "access_count_24h": 47 }
}
```

Policy evaluation is synchronous and on the critical path. Target latency p99 < 2ms for any policy that fits within the fuel budget.

### Anomaly Detector (Go)

A lightweight ML model embedded in the node process — no external service dependency. Builds a per-identity behavioral baseline from the access history stored in the audit log.

The baseline captures: access time-of-day distribution, source IP CIDR ranges, secret path access patterns, access frequency. A new access is scored against the baseline. Accesses that deviate significantly from baseline — a new source IP CIDR, access at an unusual hour, a path not previously accessed — produce an anomaly score.

The anomaly detector operates in two modes:
- **Alert mode** — anomaly scores above threshold are emitted as Prometheus metrics and logged to the audit trail; access is still granted if policy allows
- **Enforce mode** — anomaly scores above threshold cause the request to be denied; the denial is logged with the anomaly score and the baseline deviation reason

The model is deliberately simple: a per-feature deviation score aggregated with configurable weights. The point is not ML sophistication — it is that anomaly detection is a runtime enforcement concern, not a post-hoc analysis concern, and the architecture reflects that.

### Lease Manager (Go)

Every successful secret access issues a lease — a time-bounded token that authorizes continued access to a secret version without re-running policy evaluation on every read. Leases are tracked as KV entries committed through Raft:

```
Key:   lease:<lease_id>
Value: {identity, path, version, issued_at, expires_at, renewable}
```

A lease that expires is not silently ignored — it is placed on an expiry queue backed by the Raft log. When the expiry event is committed, the lease is no longer valid cluster-wide. A service holding an expired lease must re-authenticate and re-run policy evaluation to get a new one.

Lease renewal is a causal consistency write: the new lease entry causally follows the previous one, and the vector clock ensures the renewal is visible to any node that has seen the previous lease.

### Audit Engine (Go)

An append-only, hash-chained audit log committed through Raft. Every access decision — allow or deny, with full context — is an audit record:

```
Record N:
  event:        secret_access_allowed
  identity:     payments-service
  path:         services/payments/db-password
  version:      v4
  consistency:  strong
  lease_id:     lease-abc-123
  timestamp:    1712345678
  policy:       payments-policy-v2
  anomaly_score: 0.12
  prev_hash:    sha256(Record N-1)
  hash:         sha256(this record)
```

The hash chain means a record cannot be modified or deleted without invalidating all subsequent records. An operator running `meridian-cli audit verify` recomputes the chain from genesis — any break is reported with the index and the two records involved.

Because audit records are committed through Raft, the audit log is consistent across the cluster. An auditor querying any node gets the same history — there is no "the log is on the leader" problem.

### Raft Consensus Layer (Go)

Implements the Raft protocol as described in the Ongaro and Ousterhout paper, plus the pre-vote extension from the dissertation. Three roles: Leader, Follower, Candidate. Three sub-protocols: leader election, log replication, and membership change.

**Leader election** — nodes start as followers. If a follower receives no heartbeat within the election timeout (randomized 150–300ms), it becomes a candidate, increments its term, votes for itself, and sends RequestVote RPCs to all peers. If it receives votes from a majority, it becomes leader and begins sending heartbeats. The randomized timeout prevents split votes from cascading.

**Log replication** — the leader appends all writes to its log, then sends AppendEntries RPCs to all followers. When a majority of nodes have persisted the entry, it is committed and applied to the state machine. The leader sends the commit index to followers in subsequent heartbeats.

**Safety invariants enforced:**
- Election safety: at most one leader per term
- Log matching: if two logs have an entry with the same index and term, all preceding entries are identical
- Leader completeness: if an entry is committed in term T, it will appear in the logs of all leaders in terms > T
- State machine safety: if a node applies an entry at a given index, no other node applies a different entry at the same index

### Vector Clock Manager (Go)

Tracks causality for the causal consistency path. Each node maintains a vector clock — an array of logical counters, one per node. A write increments the writing node's counter. A read returns both the value and the vector clock at the time of the read.

Used specifically in Meridian for lease renewals and policy updates — operations where the client must see a causally consistent view of prior state, but where paying for a full quorum round-trip on every access is unnecessary overhead.

```
Node 1 issues lease:       VC = [1, 0, 0]
Node 2 reads lease:        VC = [1, 0, 0]
Node 2 renews lease
  (causally after reading): VC = [1, 1, 0]

Any node that has seen the renewal has also seen the original lease.
```

### Storage Engine (Rust)

An LSM (Log-Structured Merge) tree providing durable, crash-safe storage. Three components:

- **WAL** — every write is appended to the WAL (AES-256-GCM encrypted, fsync on write) before being applied to the memtable. On crash recovery, the WAL is replayed to reconstruct in-memory state.
- **Memtable** — an in-memory sorted map (BTreeMap). Writes land here after WAL. When the memtable exceeds its size threshold, it is flushed to an SSTable on disk.
- **SSTables** — immutable sorted files on disk. Reads check the memtable first, then SSTables newest to oldest. Bloom filters skip files that cannot contain the key. Background compaction merges SSTables to bound read amplification.

Rust earns its place here for the same reason it earns its place in every write-hot path: deterministic latency with no GC pauses, memory safety without runtime overhead, and precise control over memory layout for SSTable compaction.

### Replication Layer (Go)

Handles the eventual consistency path. Async gossip replication — writes applied locally are propagated to peers in the background without blocking the client. Replication is best-effort: writes buffer during a partition and are replayed when the partition heals. Eventual reads from a lagging node include `STALE_READ: true` in the response — staleness is acknowledged, not hidden.

The replication layer does not guarantee ordering across nodes in eventual mode. This is correct behavior for eventual consistency, and it is specifically tested by the chaos suite.

### Chaos Orchestrator (Python)

An adversarial test harness that actively destroys the cluster while correctness is verified. Runs only in an isolated Docker network — this is enforced at the network level, not by convention. Three categories of chaos:

- **Node kills** — SIGKILL random nodes, verify cluster continues, verify the killed node rejoins and converges
- **Network partitions** — Docker iptables rules to isolate nodes; verify majority/minority behavior per consistency level
- **Clock skew injection** — advance or retard system time; verify vector clock causality is not violated; verify Raft election stability

In the combined system, the chaos suite also runs:
- **Secret access under partition** — a service tries to fetch a credential while the cluster is partitioned; verify the majority serves, the minority returns the correct error, and no incorrect credential is ever served
- **Rotation under failover** — a secret rotation is in flight when the leader dies; verify the rotation either completes correctly or rolls back cleanly — no half-rotated state

### Linearizability Checker (Python)

A Jepsen-style verification tool. Records every operation (key, type, value, timestamp, node) during a chaos run. After the run, verifies the operation history is consistent with a linearizable execution — that there exists a total ordering consistent with real time and the single-value constraint.

The checker uses the Wing-Gong linearizability algorithm with P-compositionality optimization for per-key parallel verification of large histories.

---

## Consistency Model Per-Request (End-to-End)

### Secret Write (Strong Consistency)

```
Service: PutSecret("services/payments/db-password", value, STRONG)
  │
  ↓
Request Router
  ├─ Extract identity from mTLS cert: "payments-service"
  ├─ Policy engine: eval(identity, path, action=write) → ALLOW
  ├─ Anomaly detector: score = 0.08 (within baseline) → OK
  │
  ↓
Secret Store
  ├─ Encrypt value with per-secret DEK
  ├─ Generate new version: "v5"
  ├─ Build log entry: {secret:path:v5 = ciphertext, secret:path:current = v5}
  │
  ↓
Raft Leader
  ├─ Append log entry (index=1042, term=7)
  ├─ AppendEntries to all followers → majority ACK
  ├─ Commit index = 1042
  ├─ Apply to storage engine (Rust)
  └─ Return success + new version + lease

  └─ Audit Engine records: secret_write_allowed, identity, path, v5, VC, timestamp, hash
```

### Secret Read (Strong Consistency)

```
Service: GetSecret("services/payments/db-password", STRONG, lease_id)
  │
  ↓
Request Router
  ├─ Validate lease_id against lease store (lease still valid? not expired?)
  ├─ Policy engine: eval(identity, path, action=read) → ALLOW
  ├─ Anomaly detector: score = 0.11 → OK
  │
  ↓
Raft Leader
  ├─ Read index protocol: record current commit index (1042)
  ├─ Heartbeat to majority of followers to confirm leadership
  ├─ Wait for majority ACK
  ├─ Read secret:path:current → "v5"
  ├─ Read secret:path:v5 → ciphertext
  ├─ Decrypt with DEK
  └─ Return plaintext + lease renewal

  └─ Audit Engine records: secret_access_allowed, identity, path, v5, strong, score, hash
```

### Secret Lease Renewal (Causal Consistency)

```
Service: RenewLease(lease_id, CAUSAL, client_vc=[4,3,2])
  │
  ↓
Vector Clock Manager
  ├─ Merge client VC [4,3,2] with local VC [4,2,2] → [4,3,2]
  ├─ Increment own component → [4,4,2]
  ├─ Write new lease entry with VC [4,4,2]
  └─ Return new lease + VC [4,4,2] to client

  └─ Any node that has seen this renewal has also seen the original lease issuance.
```

### Secret Read (Eventual Consistency — Cached Sidecar)

```
Sidecar: GetSecret("services/payments/db-password", EVENTUAL)
  │
  ↓
Local node storage — no quorum, no policy re-evaluation
  ├─ Read from local state
  ├─ If node is behind leader commit index: STALE_READ = true
  └─ Return cached secret + STALE_READ flag + version

  Note: Eventual reads still validate the local lease. An expired lease is rejected
        even on the eventual path — the expiry event is committed through Raft
        and applied to all nodes before the expiry window closes.
```

---

## Quorum Calculation Under Partial Failure

For a cluster of N nodes, Raft requires a quorum of ⌈N/2⌉ + 1 nodes for any committed operation.

| Cluster Size | Quorum | Max Tolerated Failures |
|---|---|---|
| 3 | 2 | 1 |
| 5 | 3 | 2 |
| 7 | 4 | 3 |

During a partition, the majority partition continues serving strong reads and writes. The minority partition:
- **Strong secret reads/writes:** rejected — `QUORUM_UNAVAILABLE`
- **Eventual reads:** served from local state — stale but available, with `STALE_READ: true`
- **Lease validation:** expired leases are rejected even on the eventual path (expiry is committed through Raft before taking effect)
- **Policy evaluation:** runs against the local policy version; a policy update committed during a partition is not visible to the minority until healing

Meridian does not hide the partition from clients. Explicit error codes on the strong path. Explicit staleness flags on the eventual path. The client knows exactly what kind of data it is getting.

---

## Raft Log Compaction

The Raft log grows unboundedly without compaction. Log compaction in Meridian:

1. Leader takes a snapshot of the current storage engine state at a log index
2. Snapshot is written to disk (Rust storage engine serializes state, including all secret versions and current pointers, lease entries, policy versions, and audit records up to the snapshot index)
3. All log entries up to the snapshot index are deleted from the Raft log
4. New followers receive the snapshot via streaming gRPC instead of replaying the full log

The audit log is included in snapshots. An auditor restoring from a snapshot gets the full audit history — the hash chain is preserved and verifiable from genesis through the snapshot index.

---

## Module Structure

```
meridian/
├── cmd/
│   ├── node/                 ← Go node server entrypoint
│   └── meridian-cli/         ← Admin + operator CLI
├── core/
│   ├── raft/                 ← Raft state machine, leader election, log replication
│   ├── router/               ← Per-request consistency level routing + identity extraction
│   ├── vectorclock/          ← Vector clock management, merge, conflict detection
│   ├── replication/          ← Async gossip replication for eventual path
│   ├── quorum/               ← Quorum calculation, partition detection
│   └── snapshot/             ← Snapshot creation, streaming transfer
├── secrets/
│   ├── store/                ← Secret CRUD, versioning, DEK/KEK encryption
│   ├── rotation/             ← Rotation scheduler, grace period, revocation
│   └── lease/                ← Lease issuance, renewal, expiry queue
├── policy/
│   ├── engine/               ← WASM sandbox (wasmtime), fuel-bounded evaluation
│   ├── compiler/             ← Rego-inspired DSL → WASM compilation
│   └── store/                ← Policy versioning + rollback (backed by core KV)
├── observability/
│   ├── anomaly/              ← Per-identity behavioral baseline, deviation scoring
│   ├── audit/                ← Hash-chained records, Raft-committed, chain verification
│   └── metrics/              ← Prometheus instrumentation, Grafana dashboard configs
├── storage/                  ← Rust LSM storage engine (WAL, memtable, SSTable)
├── chaos/                    ← Python chaos orchestrator
├── checker/                  ← Python linearizability verifier
├── proto/                    ← Protobuf definitions (client API + inter-node)
├── docker/                   ← Multi-node cluster docker-compose
└── docs/
```

---

## References

- [Design Doc](DESIGN_DOC.md) — Raft log replication, vector clock algorithm, quorum under partial failure, chaos suite design, secret rotation protocol, policy evaluation sandbox
- [Tradeoffs](TRADEOFFS.md) — strong vs causal vs eventual, LSM vs RocksDB, WASM sandbox vs in-process evaluation, ML anomaly detection design
- [Runbook](RUNBOOK.md) — split-brain recovery, log divergence repair, snapshot restore, secret rotation rollback, audit chain verification
- [Raft paper](https://raft.github.io/raft.pdf) — Ongaro & Ousterhout
- [Vector clocks](https://lamport.azurewebsites.net/pubs/time-clocks.pdf) — Lamport, "Time, Clocks, and the Ordering of Events"
- [Linearizability](https://cs.brown.edu/~mph/HerlihyW90/p463-herlihy.pdf) — Herlihy & Wing
