# Meridian Roadmap

**Current Phase:** Phase 5 — Observability
**Last Updated:** April 2026

---

## Design Philosophy

Distributed systems correctness cannot be verified incrementally from a happy path. The chaos suite is not a Phase 6 concern — it runs from Phase 2 onward, against every component as it is added. Each phase adds one distributed systems primitive. Each primitive is chaos-tested before the next one is layered on top. A cluster that cannot survive a node kill does not get vector clocks until it can.

The rule: a phase is complete when the chaos suite passes, not when the unit tests pass. Happy path tests are necessary but not sufficient.

---
## Status

| Component | Status | Description |
|---|---|---|
| **Project Structure** | ✅ Complete | Modular monorepo, CI/CD, multi-node Docker cluster |
| **Storage Engine (Rust)** | ✅ Complete | LSM tree, WAL (AES-256-GCM), memtable, SSTable compaction |
| **Raft Consensus (Go)** | ✅ Complete | Leader election, log replication, pre-vote, snapshot, compaction |
| **gRPC API (Go)** | ✅ Complete | Secret CRUD + KV Get/Put/Delete with per-request consistency level |
| **Strong Consistency** | ✅ Complete | Quorum reads + writes via Raft — default for all secret writes |
| **Eventual Consistency** | ✅ Complete | Async replication, stale-flagged reads |
| **Causal Consistency** | ✅ Complete | Vector clocks, causality tracking, conflict resolution |
| **Secret Store** | ✅ Complete | Versioned secrets, TTL, encrypted at rest and in transit |
| **Automatic Rotation** | ✅ Complete | Schedule-based + on-demand, grace period, quorum-committed rotation events |
| **Lease Manager** | ✅ Complete | TTL-bound access, lease renewal, expiry queue |
| **Policy Engine (WASM)** | ✅ Complete | Rego-inspired DSL, WASM sandbox via wasmtime, deny-by-default |
| **Anomaly Detector (ML)** | 📋 Planned | Per-identity behavioral baseline, deviation scoring, optional auto-deny |
| **Tamper-Evident Audit Log** | 📋 Planned | Hash-chained audit records committed through Raft |
| **Chaos Orchestrator (Python)** | 📋 Planned | Node kills, network partitions, clock skew, secret access under partition |
| **Linearizability Checker (Python)** | 📋 Planned | Jepsen-style history verification |
| **Prometheus + Grafana** | 📋 Planned | Replication lag, consensus latency, quorum health, secret access rates |
| **Admin CLI** | 📋 Planned | Cluster topology, secret management, policy upload, audit query, leader transfer |

**Current Milestone:** Project structure established. Beginning Rust storage engine and Raft leader election.

---

## Roadmap

### Phase 1 — Storage Engine and Single-Node Foundation
**Goal:** A single node that stores and retrieves key-value pairs durably.

- [ ] Rust LSM storage engine: WAL (AES-256-GCM), memtable, SSTable, compaction, crash recovery
- [ ] Go gRPC API: Get, Put, Delete — single node, no replication
- [ ] Integration tests: write 1,000 keys, SIGKILL, restart, verify all keys present
- [ ] Benchmarks: write throughput, read latency p50/p99 with and without bloom filters

**Exit:** Write 10,000 keys. Kill the node. Restart. All 10,000 keys readable. Write throughput > 10,000 ops/sec. Read p99 < 5ms for a 1M key dataset with bloom filters.

---

### Phase 2 — Raft Consensus: Leader Election
**Goal:** A 3-node cluster that elects a leader and maintains leadership under node failures.

- [ ] Raft state machine: Follower, Candidate, Leader
- [ ] Leader election: RequestVote RPC, randomized election timeout 150–300ms
- [ ] Pre-vote optimization
- [ ] Heartbeat, term management, mTLS between nodes
- [ ] Node identity verification before cluster join
- [ ] Election audit log
- [ ] Chaos: node kill during election — new leader within 5s
- [ ] Chaos: simultaneous follower kills — leader steps down when quorum lost

**Exit:** Kill the leader. New leader elected within 5 seconds. Kill 2 of 3 nodes — no leader elected (no quorum). Restart both — cluster reforms correctly.

---

### Phase 3 — Raft Log Replication and Strong Consistency
**Goal:** Writes committed to a quorum. Committed writes survive any single node failure.

- [ ] AppendEntries RPC with batching
- [ ] Log persistence, commit index tracking, read index protocol
- [ ] Log divergence repair, snapshot creation and streaming transfer
- [ ] Log compaction
- [ ] Chaos: leader crash mid-replication — no committed writes lost
- [ ] Chaos: network partition 2+1 — majority serves, minority rejects
- [ ] Linearizability checker: strong consistency writes verified

**Exit:** 1,000 strong writes. Kill leader after 500 commit. Restart. All 500 committed writes present on all nodes. Linearizability checker finds no violations.

---

### Phase 4 — Secrets Management Layer
**Goal:** Strongly consistent secrets on top of the consensus engine.

- [ ] Secret store: versioned CRUD, TTL, encryption at rest and in transit
- [ ] Automatic rotation: schedule-based, on-demand, grace period
- [ ] Lease manager: TTL-bound access, renewal, expiry queue backed by Raft log
- [ ] Secret versioning and audit trail (pre-hash-chain — plain append)
- [ ] CLI: `secret put`, `secret get`, `secret rotate`, `secret versions`
- [ ] Chaos: secret rotation during leader failover — verify no dual-valid-version window
- [ ] Chaos: lease expiry under partition — verify expired leases are rejected cluster-wide

**Exit:** Rotate a secret during a network partition. Verify the old version is not accessible from the majority partition after the grace period expires. Verify the new version is accessible on all nodes after partition heals. No window where both versions are simultaneously valid across a partition.

---

### Phase 5 — Policy Engine (WASM Sandbox)
**Goal:** Every secret access is a policy decision. Evaluation is sandboxed and cannot affect the node.

- [ ] Rego-inspired DSL: parser, AST, type checker
- [ ] WASM compiler: DSL → WASM via wasmtime
- [ ] Policy evaluation on the read path: deny-by-default
- [ ] Policy versioning and rollback via core KV store
- [ ] Contextual evaluation: service identity, path, source IP, time of day, access history
- [ ] CLI: `policy put`, `policy eval`, `policy rollback`
- [ ] Chaos: policy update during partition — verify consistent policy evaluation cluster-wide

**Exit:** Upload a policy that allows access only during business hours. Attempt access outside that window — denied. Attempt access from a non-datacenter IP — denied. Roll back the policy — previous behavior restored. Policy evaluation latency p99 < 2ms.

---

### Phase 6 — Causal Consistency and Vector Clocks
**Goal:** Causal consistency path tracks causality and detects concurrent writes.

- [ ] Vector clock implementation: per-node counter array, merge on receive
- [ ] Causal write and read protocol
- [ ] Causality enforcement with bounded retry
- [ ] Concurrent write detection and LWW conflict resolution
- [ ] Conflict log — every concurrent write recorded, not silently resolved
- [ ] Chaos: concurrent writes from partitioned nodes — conflict detection on heal verified

**Exit:** Write key "x" from Node 1. Read "x" from Node 2 — receive VC. Write key "y" from Node 2 with that VC. Any subsequent causal read of "y" sees "x" at the value Node 2 saw. Chaos suite verifies this under partitions.

---

### Phase 7 — Eventual Consistency and Async Replication
**Goal:** Eventual path serves reads from any node with explicit staleness acknowledgment.

- [ ] Async gossip replication, replication lag tracking
- [ ] Stale read detection and STALE_READ response flag
- [ ] Partition healing: buffered writes replayed on reconnect
- [ ] Chaos: partition 2+1, write to majority, read from minority — STALE_READ=true
- [ ] Chaos: heal partition — minority converges within replication window, STALE_READ=false

**Exit:** Partition 2+1. Write 100 keys to majority. Read from minority — STALE_READ=true on all. Heal partition. Within 30 seconds, minority returns STALE_READ=false with correct values. No writes lost.

---

### Phase 8 — Observability, Anomaly Detection, and Full Chaos Suite
**Goal:** The system is fully observable, anomalies are detected at runtime, and correctness is mathematically verified.

- [ ] ML anomaly detector: per-identity behavioral baseline, deviation scoring, optional auto-deny
- [ ] Tamper-evident audit log: hash-chained records, Raft-committed
- [ ] Prometheus metrics: replication lag, quorum health, secret access rates, policy evaluation latency, anomaly scores
- [ ] Grafana dashboards: cluster health, consistency level distribution, rotation status, lease queue depth
- [ ] Full linearizability checker: Wing-Gong algorithm, P-compositionality optimization
- [ ] Chaos: secret access under network partition — full scenario battery
- [ ] Chaos: anomaly injection — access from unexpected source triggers detection and alert
- [ ] 30-minute chaos run — no linearizability violations

**Exit:** Chaos suite runs the full scenario battery for 30 minutes. Linearizability checker finds no violations. Anomaly detector fires within 3 accesses of a behavioral deviation. Audit log hash chain verifiable end-to-end. This is the exit criterion that matters. All others are prerequisites.

---

## v1 → v2 → v3 Summary

| Version | Scope |
|---|---|
| **v1** (Phases 1-4) | 3-node cluster, Raft consensus, strong + eventual consistency, chaos suite |
| **v2** (Phases 5-6) | Causal consistency, vector clocks, linearizability checker, full observability |
| **v3** (Post-roadmap) | Geo-distributed multi-region, cross-region Raft, bounded staleness, HLC timestamps |

---

## Risk Management

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Raft log divergence bug after leader crash | High | Critical | Phase 3 exit criteria requires verifying no committed write loss — cannot proceed without this |
| Vector clock causality violation under concurrent writes | High | Critical | Phase 5 chaos suite tests specifically for causality violations |
| LSM read amplification grows without compaction | Medium | Medium | Compaction benchmarks in Phase 1 before building replication on top |
| Linearizability checker too slow for large histories | Medium | Medium | P-compositionality optimization, bounded concurrency in chaos scenarios |
| Clock skew breaks Raft election stability | Low | High | Pre-vote implemented in Phase 2 specifically to mitigate this |

**The non-negotiables:** no committed write is ever lost after a node crash. No linearizability violation is ever produced by the strong consistency path. No causality violation is ever produced by the causal consistency path. These three invariants are the product. The chaos suite must verify all three before any phase is considered complete.
