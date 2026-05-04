# Meridian Tradeoffs

**Purpose:** Every major design decision in the combined system — what was rejected and why.
**Last Updated:** April 2026

---

## Why Document Tradeoffs?

Every distributed system is a collection of tradeoffs. A secrets management platform built on a distributed store is a collection of two systems' worth of tradeoffs — and the interaction between them creates a third set that neither system would face alone. The CAP theorem is not a constraint you work around; it is a constraint you choose. Strong consistency for secret writes means unavailability during partitions, by definition. WASM sandboxing for policy evaluation means additional per-request overhead, by design. An ML anomaly detector means a false positive rate you must accept and tune, by necessity.

Understanding which tradeoff was made and why is what separates a distributed systems engineer from someone who has deployed distributed systems.

---

## Strong Consistency: Correctness vs. Availability

**Chosen:** Raft-based strong consistency via quorum reads and writes (read index protocol)
**Alternative considered:** Multi-Paxos, single-leader reads without quorum confirmation, leader leases

Strong consistency via Raft means that during a network partition, the minority partition cannot serve reads or writes on the strong path. This is not a bug — it is the definition of strong consistency under CAP. For secrets management specifically, this is the only acceptable behavior. A credential served from a partitioned minority that has missed a rotation event may be stale in a security-critical way: the old credential may have been revoked, or rotated after a compromise. The `QUORUM_UNAVAILABLE` error is actionable. A silently wrong credential is a security incident.

The read index protocol is the implementation decision that makes strong reads correct. Without it, a partitioned leader could serve reads from local state after a new leader has been elected in the majority partition. The read index protocol requires the leader to confirm its leadership via a heartbeat round-trip to a quorum before serving any read. This adds one network round-trip to every strong read.

Leader leases — where the leader assumes it remains leader for a bounded time window and serves reads without a confirmation round-trip — were evaluated and rejected for v1. Leases require bounded clock drift guarantees. Without hardware-level clock synchronization equivalent to Spanner's TrueTime, the bound cannot be enforced reliably. A clock skew event could cause a lapsed leader to serve reads it should not serve. For a generic KV store, this risk might be acceptable. For a secrets platform, it is not.

---

## Causal Consistency: Lease Renewals and Policy Reads

**Chosen:** Vector clocks with last-write-wins conflict resolution
**Alternative considered:** Lamport timestamps, hybrid logical clocks, CRDTs

Causal consistency with vector clocks is used for two operations in Meridian: lease renewals and policy reads. Both share the same requirement — the client needs a causally consistent view of prior writes (the prior lease, the prior policy version) without paying for a full quorum round-trip on every access.

Lamport timestamps provide a total order over events but do not capture causality precisely. Two events with timestamps T and T+1 may be entirely unrelated. Vector clocks capture the actual causal relationship.

Hybrid logical clocks (HLC) combine physical time with logical counters, enabling timestamp-based queries ("what was the policy version at time T?"). This is a v2 consideration when geo-distributed policy reads require timestamp-anchored consistency. For v1, vector clocks are simpler and sufficient.

CRDTs were evaluated for the conflict resolution strategy. Last-write-wins is incorrect for some data types — a shopping cart should merge concurrent additions rather than overwrite. For secrets and leases, LWW is correct: the most recent credential version wins; the most recent lease renewal wins. CRDTs add complexity without benefit for these access patterns and are documented as a v2 extension for application-layer data.

The client complexity cost of causal consistency is real — clients must carry and propagate the vector clock. The gRPC API makes this explicit: the `vector_clock` field in causal requests is required, not optional. This is intentional. A client that does not propagate its vector clock cannot make causal reads.

---

## Eventual Consistency: Cached Credential Reads

**Chosen:** Async gossip replication, stale reads explicitly flagged
**Alternative considered:** Tunable staleness window, read repair

The eventual consistency path is used for one specific pattern: a sidecar agent doing a cached credential refresh, where the service already holds the credential and is proactively refreshing before expiry. In this context, a slightly stale read is acceptable — the service already has the credential and is not making an authorization decision based on this read.

A tunable staleness window (serve eventual reads only if the node is within N seconds of the leader) adds complexity without solving the fundamental problem. If the staleness window is exceeded during a partition, the node must reject reads — eliminating the availability advantage of the eventual path. Meridian implements pure eventual for v1 and documents bounded staleness as a v2 extension.

Stale reads are never silent. Every eventual response includes `STALE_READ: true` when the serving node is behind the leader's commit index. A client that receives `STALE_READ: true` and cannot tolerate staleness knows to retry with strong consistency. This is a better developer experience than silently returning a potentially stale credential.

One important constraint on the eventual path for secrets: lease validation is not relaxed. An expired lease is rejected even on the eventual path. The lease expiry event is committed through Raft and applied to all nodes before taking effect — a node behind the commit index applies pending log entries (including the expiry event) before serving eventual reads for the affected key.

---

## Secret Rotation: Atomic Log Entry vs. Two-Step Write

**Chosen:** Single atomic Raft log entry for the rotation (new version + current pointer update + revocation schedule)
**Alternative considered:** Two-step write (version then pointer), distributed transaction, saga pattern

The two-step write approach — write the new version, then update the current pointer — creates a window where the system is in a partially-rotated state. If the leader crashes between the two writes, the cluster is left with an inconsistent view of which version is current. This is not a theoretical concern; it is the failure mode that happens in practice under chaos.

A distributed transaction (two-phase commit) across the two writes was considered. 2PC adds coordinator complexity and a blocking commit phase that reduces availability — the opposite of what a secrets platform needs during a rotation.

The saga pattern — compensating transactions that undo a partial rotation — was evaluated. A saga that fails mid-way must execute a compensating action (roll back to the old version). Rollback logic under partition is complex: the rollback write may also fail to reach quorum, leaving the rollback itself in a partial state.

The single atomic log entry is the correct solution because Raft's state machine semantics guarantee: either the entire entry is applied on a node, or none of it is. There is no partial application. The rotation is not split across multiple log entries. The window of inconsistency is zero.

---

## WASM Policy Sandbox vs. In-Process Evaluation

**Chosen:** WASM sandbox via wasmtime with fuel-based termination
**Alternative considered:** In-process Go interpreter, OPA (Open Policy Agent) as a sidecar, Lua embedding

In-process Go evaluation (a reflection-based rule engine or an interpreted DSL) is fast but provides no isolation. A malformed policy — one that panics, loops infinitely, or allocates aggressively — can degrade or crash the node. For a platform that runs beneath everything else, a policy-induced node crash is unacceptable. If Meridian goes down, nothing can authenticate.

OPA as a sidecar process was considered. OPA is battle-tested and production-proven. The reasons for the custom WASM implementation: the portfolio goal is to demonstrate policy engine architecture, not OPA deployment. An embedded policy engine also eliminates a network hop on the evaluation path and removes a failure dependency — a Meridian node does not fail policy evaluations because its OPA sidecar is unreachable.

Lua embedding was considered. Lua is a widely-used embedded scripting language with reasonable isolation properties. However, Lua does not have the WASM ecosystem's standardized isolation guarantees, and the compiled WASM artifact is more portable for policy distribution across heterogeneous nodes.

The fuel model was chosen over a goroutine-based timer for terminating runaway policies. A goroutine timer can be bypassed by a WASM module that avoids yielding to the Go scheduler. Wasmtime's fuel model is enforced at the instruction level — the WASM runtime counts instructions and terminates the instance at the budget boundary, regardless of scheduling behavior.

The tradeoff: WASM evaluation has a higher fixed overhead than in-process evaluation. The target p99 of 2ms for policy evaluation accepts this overhead. The isolation guarantee is worth the latency.

---

## Rust for the Storage Engine

**Chosen:** Custom Rust LSM implementation
**Alternative considered:** RocksDB (via cgo FFI), BadgerDB (Go), BoltDB (Go)

The storage engine is the write hot path for every secret write, lease update, audit record, and Raft log entry. At high throughput, GC pauses in Go produce write latency spikes. A secrets platform with periodic write latency spikes is not a reliable primitive for the services built on top of it.

RocksDB via cgo FFI would be correct — RocksDB is battle-tested and used by CockroachDB, TiKV, and many others. The reason for the custom Rust implementation: the project goal is to demonstrate storage engine internals. A custom LSM shows understanding of memtable design, SSTable format, bloom filter sizing, compaction strategy selection, and WAL format. Configuring RocksDB knobs demonstrates familiarity with a specific library. These are different skills, and for a project demonstrating distributed systems depth, the custom implementation is the correct choice.

BadgerDB and BoltDB are Go-native and eliminate the cgo boundary, but they share the GC problem on the write path. BadgerDB uses an LSM-like structure but is not as tunable. BoltDB is a B-tree, which trades write amplification for better read characteristics than an LSM — incorrect for a write-heavy secrets platform.

The accepted tradeoff: the custom Rust LSM will have bugs that RocksDB does not have. These bugs are caught by the chaos suite and the linearizability checker, which is exactly the purpose of those components.

---

## ML Anomaly Detection: Embedded vs. External

**Chosen:** Lightweight embedded Go model with per-identity behavioral baseline
**Alternative considered:** External ML service (Python, TensorFlow Serving), statistical outlier detection only, no anomaly detection

An external ML service introduces a network dependency on every secret access. If the anomaly detection service is unavailable, Meridian must either deny all requests (too restrictive) or bypass detection (defeats the purpose). Neither is acceptable for a platform that must be highly available.

The embedded model is deliberately simple: per-feature deviation scoring with configurable weights. This is not a sophisticated ML model — it is a practical anomaly detection mechanism that operates with no external dependency, bounded memory, and bounded CPU per evaluation. Sophistication is not the goal. Catching behavioral anomalies — a new source IP CIDR, off-hours access, a path not previously accessed — is the goal, and the simple model achieves it.

The false positive rate is a real concern. A system that denies legitimate access because it looks anomalous is worse than no anomaly detection. The two-mode design (alert vs. enforce) addresses this: operators run in alert mode first, tune the thresholds using the Prometheus metrics, and enable enforce mode when confident in the baseline. This is not a theoretical consideration — it is the operational workflow, and it is documented in the runbook.

Statistical outlier detection only (without ML) was considered. A pure statistical approach (z-score on access frequency, IP range checks) misses behavioral patterns that require multi-feature reasoning. The ML baseline is not much more complex to implement but captures correlations the statistical approach cannot.

---

## Chaos Suite: Destructiveness vs. Safety

**Chosen:** Destructive chaos in isolated Docker network, never run against real infrastructure
**Alternative considered:** Fault injection via library hooks, chaos in unit tests, mock network partitions

The chaos suite uses actual iptables network manipulation, actual SIGKILL process termination, and actual system clock modification. These are the same mechanisms that cause real failures in production. A chaos suite that simulates failures via mock injection does not test the same code paths as real failures.

The isolation constraint is enforced at the network level: the Docker network has no external routes. The chaos orchestrator verifies it is running in an isolated environment before executing any destructive operation. Running the chaos suite against a real cluster by accident is a network-level impossibility, not a documented warning that can be ignored.

The Python chaos orchestrator is separate from the Go node implementation. Chaos must be independent from the system under test — an embedded chaos framework cannot SIGKILL its own process or manipulate iptables at the OS level without affecting the system it is testing.

---

## Tamper-Evident Audit Log: Hash Chain vs. Signed Records

**Chosen:** SHA-256 hash chain (each record hashes the previous)
**Alternative considered:** Per-record HMAC signing, Merkle tree audit log, external append-only log service

Per-record HMAC signing (each record is independently signed with a cluster key) provides tamper evidence per record but does not detect insertion or deletion — a record can be removed from the sequence without breaking any individual record's signature. The hash chain detects any modification, insertion, or deletion because a gap or change in the sequence breaks the chain from that point forward.

A Merkle tree audit log provides efficient membership proofs — an auditor can verify that a specific record is in the log without downloading the entire log. This is a v2 consideration when compliance requirements demand efficient third-party auditing. For v1, the full chain verification is sufficient.

An external append-only log service (a cloud logging backend with append-only guarantees) introduces an external dependency that creates a failure mode — if the external service is unavailable, audit records cannot be written. Meridian's audit log is committed through Raft, so it is only unavailable when the cluster itself is unavailable. The audit log failure mode is the same as the secrets failure mode.

---

## Pre-Vote vs. Standard Raft Election

**Chosen:** Pre-vote extension (Ongaro dissertation, not original paper)
**Alternative considered:** Standard Raft election

Without pre-vote, a node that was partitioned for an extended period accumulates a high term from repeated failed elections. When it reconnects, its higher term causes the current leader to step down unnecessarily — a disruption to a stable cluster serving live secret requests.

Pre-vote prevents this by requiring a node to confirm it could win an election before starting one. If it cannot get a pre-vote majority, it does not increment the term and does not start the election.

The tradeoff: pre-vote adds one extra message round-trip to every election. Under normal operation, elections are rare — only when the leader fails. The additional latency is negligible compared to the stability benefit. For a secrets platform where leader transitions affect all in-flight secret requests, stability is worth a single extra round-trip.

---

## Summary

| Decision | Chosen | Alternative | Core Reason |
|---|---|---|---|
| Strong consistency | Raft quorum reads (read index) | Leader lease | Read index is correct under any clock behavior; leases require clock bounds |
| Secret rotation | Single atomic log entry | Two-step write, 2PC, saga | Zero window of inconsistency; Raft state machine semantics guarantee atomicity |
| Causal consistency | Vector clocks + LWW | HLC, CRDTs | Vector clocks capture causality; LWW correct for secrets and leases |
| Eventual consistency | Async gossip, stale-flagged | Bounded staleness | Simpler; staleness is acknowledged, not hidden; leases still validated |
| Policy sandbox | WASM + wasmtime + fuel model | In-process Go, OPA sidecar | Isolation without network hop; fuel model prevents runaway policies |
| Anomaly detection | Embedded Go ML, per-identity baseline | External service, pure statistics | No external dependency; operational failure mode matches cluster failure mode |
| Audit log | SHA-256 hash chain, Raft-committed | HMAC per record, Merkle tree | Chain detects insertion/deletion; Raft-committed matches cluster availability |
| Storage engine | Custom Rust LSM | RocksDB, BadgerDB | Demonstrates storage internals; no GC on write path |
| Chaos suite | Destructive, OS-level, isolated Docker | Mock injection | Same mechanisms as real failures; isolation is enforced, not documented |
| Election protocol | Pre-vote + standard Raft | Standard Raft only | Prevents partitioned node from disrupting stable cluster |
