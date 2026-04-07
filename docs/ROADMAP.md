# Meridian Roadmap

**Current Phase:** Phase 1 — Storage Engine and Single-Node Foundation
**Last Updated:** April 2026

---

## Design Philosophy

Distributed systems correctness cannot be verified incrementally from a happy path. The chaos suite is not a Phase 6 concern — it runs from Phase 2 onward, against every component as it is added. Each phase adds one distributed systems primitive. Each primitive is chaos-tested before the next one is layered on top. A cluster that cannot survive a node kill does not get vector clocks until it can.

The rule: a phase is complete when the chaos suite passes, not when the unit tests pass. Happy path tests are necessary but not sufficient.

---

## Phase 1: Storage Engine and Single-Node Foundation

**Goal:** A single node that stores and retrieves key-value pairs durably. The foundation everything else builds on.

- [x] Project structure, CI/CD, multi-node Docker cluster skeleton
- [ ] Rust LSM storage engine
  - [ ] WAL (write-ahead log, AES-256-GCM encryption, fsync on write)
  - [ ] Memtable (BTreeMap, configurable size threshold)
  - [ ] SSTable format (sorted key-value pairs, bloom filter per file)
  - [ ] Memtable flush to SSTable (background goroutine)
  - [ ] Read path (memtable → SSTables newest to oldest, bloom filter optimization)
  - [ ] Crash recovery (WAL replay on startup)
- [ ] Go gRPC API (Get, Put, Delete — single node, no replication yet)
- [ ] Protobuf schema (KVRequest, KVResponse, ConsistencyLevel enum)
- [ ] Integration tests (write 1000 keys, crash node, restart, verify all keys present)
- [ ] Benchmarks (write throughput, read latency p50/p99 with and without bloom filters)

**Exit criteria:** Write 10,000 keys. SIGKILL the node. Restart. All 10,000 keys are readable. Write throughput > 10,000 ops/sec on NVMe SSD. Read p99 < 5ms for a 1M key dataset with bloom filters enabled.

---

## Phase 2: Raft Consensus — Leader Election

**Goal:** A 3-node cluster that elects a leader and maintains leadership under node failures.

- [ ] Raft state machine (Follower, Candidate, Leader roles)
- [ ] Leader election (RequestVote RPC, randomized election timeout 150-300ms)
- [ ] Pre-vote optimization (prevents disruption from reconnecting partitioned nodes)
- [ ] Heartbeat (AppendEntries with no entries — leadership keep-alive)
- [ ] Term management (reject stale messages from old terms)
- [ ] mTLS between nodes (node identity certificates, mutual authentication)
- [ ] Node identity verification before cluster join
- [ ] Election audit log (every election, every vote, term history)
- [ ] CLI: `meridian-cli cluster status`, `meridian-cli leader transfer`
- [ ] Chaos: node kill during election — verify new leader elected within 5s
- [ ] Chaos: simultaneous follower kills — verify leader step-down when quorum lost

**Exit criteria:** Kill the leader. A new leader is elected within 5 seconds, confirmed by the linearizability checker. Kill 2 of 3 nodes simultaneously. The remaining node cannot elect a leader (no quorum). Restart the two killed nodes. The cluster reforms, elects a leader, and serves requests correctly.

---

## Phase 3: Raft Log Replication and Strong Consistency

**Goal:** Writes are replicated to a quorum of nodes. Committed writes survive any single node failure.

- [ ] AppendEntries RPC (log replication, batch up to max entries)
- [ ] Log persistence (Raft log stored in WAL, recoverable after crash)
- [ ] Commit index tracking (leader commits when quorum ACKs)
- [ ] Read index protocol (quorum confirmation before serving strong reads)
- [ ] Log divergence repair (nextIndex probe, follower log overwrite)
- [ ] Snapshot creation (storage engine serialization at a log index)
- [ ] Snapshot transfer (streaming gRPC, chunk-based, resumable)
- [ ] Log compaction (truncate log entries before snapshot index)
- [ ] Chaos: leader crash mid-replication — verify no committed writes lost
- [ ] Chaos: network partition 2+1 — verify majority serves, minority rejects
- [ ] Linearizability checker: verify strong consistency writes are linearizable

**Exit criteria:** Write 1000 keys with strong consistency. Kill the leader after 500 writes commit. Restart the leader. Verify all 500 committed writes are present on all nodes. Verify the 500 in-flight writes that did not reach quorum before the kill are either present on all nodes (if they were committed) or absent on all nodes (if they were not). The linearizability checker must find no violations.

---

## Phase 4: Eventual Consistency and Async Replication

**Goal:** Eventual consistency path serves reads from any node, handles partitions with stale-flagged reads.

- [ ] Async gossip replication (writes propagate to peers in background, no client blocking)
- [ ] Replication lag tracking (per-peer lag metric, surfaced in Prometheus)
- [ ] Stale read detection (node knows it is behind leader's commit index)
- [ ] STALE_READ response flag (client knows the read may be stale)
- [ ] Partition healing (buffered writes replayed on reconnect, up to buffer limit)
- [ ] Eventual convergence verification (all nodes reach same state after partition heals)
- [ ] Chaos: partition 2+1, write to majority, verify eventual read on minority returns stale flagged
- [ ] Chaos: heal partition, verify minority converges within replication window
- [ ] Chaos: write storm (1000 concurrent eventual writes), verify convergence

**Exit criteria:** Partition a 3-node cluster 2+1. Write 100 keys to the majority. Read the same keys from the minority with eventual consistency — all reads return STALE_READ=true. Heal the partition. Within 30 seconds, all reads from the minority return STALE_READ=false and the correct values. No writes are lost.

---

## Phase 5: Causal Consistency and Vector Clocks

**Goal:** Causal consistency path tracks causality via vector clocks and detects concurrent writes.

- [ ] Vector clock implementation (per-node counter array, merge on receive)
- [ ] Causal write (increment local VC component, attach VC to write)
- [ ] Causal read (verify local VC ≥ client VC, return value + current VC)
- [ ] Causality enforcement (block causal read if local VC < client VC, with bounded retry)
- [ ] Concurrent write detection (neither VC dominates the other → conflict)
- [ ] LWW conflict resolution (wall clock, then node ID tiebreaker)
- [ ] Conflict log (every concurrent write, both values, resolution reason)
- [ ] Chaos: concurrent writes from two partitioned nodes — verify conflict detection on heal
- [ ] Chaos: causal read after partition — verify causality is not violated
- [ ] Linearizability checker extension: verify causal reads satisfy monotonic read guarantee

**Exit criteria:** Write key "x" from Node 1 (VC=[1,0,0]). Read key "x" from Node 2 — client receives VC=[1,0,0]. Write key "y" from Node 2 with client VC=[1,0,0] → write carries VC=[1,1,0]. Any subsequent causal read of "y" is guaranteed to see "x" at its value when Node 2 read it. The chaos suite verifies this guarantee holds under concurrent writes and network partitions.

---

## Phase 6: Chaos Suite and Linearizability Verification

**Goal:** The chaos suite is fully automated and the linearizability checker mathematically verifies correctness.

- [ ] Automated chaos runner (scenario suite, parallelizable, report generation)
- [ ] Full linearizability checker (Wing-Gong algorithm, P-compositionality optimization)
- [ ] Chaos scenarios: all Phase 2-5 scenarios automated and reproducible
- [ ] Clock skew injection (advance/retard node clocks by configurable amounts)
- [ ] Byzantine fault simulation (node sends incorrect values — verify cluster rejects or isolates)
- [ ] Prometheus metrics: replication lag, consensus latency, election frequency, quorum health
- [ ] Grafana dashboards: cluster health, per-node write throughput, consistency level distribution
- [ ] Benchmarking dashboard (Python): throughput vs consistency level, latency under chaos

**Exit criteria:** The chaos suite runs the full scenario battery (node kills, partitions, clock skew, write storms) for 30 minutes without the linearizability checker finding a single violation. This is the exit criterion that matters. All others are prerequisites.

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