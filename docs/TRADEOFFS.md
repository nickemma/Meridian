# Meridian Tradeoffs

**Purpose:** Every major design decision, what was rejected, and why.
**Last Updated:** April 2026

---

## Why Document Tradeoffs?

Every distributed system is a collection of tradeoffs. The CAP theorem is not a constraint you work around — it is a constraint you choose. Strong consistency means unavailability during partitions, by definition. Eventual consistency means stale reads, by design. Causal consistency means the client carries state (the vector clock) and the system must enforce causality, at a performance cost. Understanding which tradeoff was made, and why, is what separates a distributed systems engineer from someone who has deployed distributed systems.

---

## Strong Consistency: Correctness vs. Availability

**Chosen:** Raft-based strong consistency via quorum reads and writes
**Alternative considered:** Multi-Paxos, single-leader reads without quorum confirmation

Strong consistency via Raft means that during a network partition, the minority partition cannot serve reads or writes. This is not a bug — it is the definition of strong consistency under CAP. A system that claims strong consistency but serves reads from a partitioned minority is not strongly consistent. Meridian makes this explicit: the minority partition returns `QUORUM_UNAVAILABLE` on strong reads and writes.

The read index protocol is the specific implementation decision that makes strong reads correct. Without it, a partitioned leader could serve reads from its local state after the partition has caused a new leader to be elected in the majority partition. Two leaders serving concurrent reads with different values is a linearizability violation. The read index protocol requires the leader to confirm its leadership (via a heartbeat round-trip to a quorum) before serving a read. This adds one network round-trip to every strong read.

Many systems optimize this with leader leases (the leader assumes it is the leader for a bounded time period and serves reads locally). Meridian does not implement leader leases in v1 — they require bounded clock drift guarantees, and without hardware-level clock synchronization (like Spanner's TrueTime), the bound is hard to enforce. The read index protocol is slower but correct under any clock behavior.

---

## Causal Consistency: Semantic Power vs. Client Complexity

**Chosen:** Vector clocks with last-write-wins conflict resolution
**Alternative considered:** Lamport timestamps, hybrid logical clocks, CRDTs

Causal consistency with vector clocks is the strongest consistency model available without a central coordinator. It guarantees that if you read a value, any subsequent write you make will be visible to anyone who reads your write. It does not guarantee that concurrent writes are handled in any particular way — that requires application-level conflict resolution.

Lamport timestamps provide a total order over events, which sounds stronger, but they do not capture causality precisely. Two events with Lamport timestamps T and T+1 may not be causally related — T+1 may have happened on a completely unrelated code path. Vector clocks capture the actual causal relationship, not just a total order.

Hybrid logical clocks (HLC) combine physical time with logical counters to provide both causality and a close approximation of wall clock time. They are useful when you need to query "what was the state at time T?" — something pure vector clocks cannot answer. HLCs are a v2 consideration when geo-distributed read routing requires timestamp-based consistency.

CRDTs (Conflict-free Replicated Data Types) handle concurrent writes without conflict — data structures like sets, counters, and maps that can always be merged. They are the correct solution when the application semantics require merge rather than overwrite. Last-write-wins is incorrect for a shopping cart (concurrent adds should merge) but correct for a configuration value (the most recent configuration wins). Meridian uses LWW in v1 because it is simpler to implement correctly and covers the most common use cases. CRDTs are a documented extension point.

The client complexity cost of causal consistency is real: clients must carry and propagate the vector clock. This is not optional — a client that does not propagate its vector clock cannot make causal reads. The gRPC API makes this explicit: the `vector_clock` field in causal requests is required, not optional.

---

## Eventual Consistency: Availability vs. Staleness

**Chosen:** Async gossip replication with stale-read acknowledgment
**Alternative considered:** Tunable staleness window, read repair

The eventual consistency path in Meridian is deliberately weak: any node serves any read from its local state, regardless of how far behind the leader it is. Writes on the eventual path are applied locally and replicated asynchronously. There is no staleness bound in v1.

A tunable staleness window (serve reads only if the node is within N seconds of the leader) adds complexity without solving the fundamental problem. If the window is 5 seconds, a partitioned node that has been isolated for 6 seconds must reject reads — at which point the availability advantage of eventual consistency disappears. The choice is between eventual (stale but always available) and bounded-staleness (slightly less stale, sometimes unavailable). Both are valid; Meridian implements eventual in v1 and documents bounded-staleness as a v2 extension.

Stale reads are flagged: eventual consistency responses include a `STALE_READ: true` header when the serving node's state is behind the leader's commit index. Clients that cannot tolerate stale reads know to retry with strong consistency. This is a better developer experience than silently returning stale data.

Read repair (updating a stale node's value when a client reads from it with a fresh value from another node) is an optimization that improves convergence speed. It is a v2 optimization — in v1, convergence is handled entirely by the gossip replication layer.

---

## Rust for the Storage Engine

**Chosen:** Rust LSM implementation
**Alternative considered:** RocksDB (via cgo FFI), BadgerDB (Go), custom Go LSM

The storage engine is the write hot path. Every Raft commit, every eventual consistency write, every snapshot writes through the storage engine. At high throughput — thousands of writes per second — GC pauses in Go become write latency spikes. A distributed KV store that produces periodic write latency spikes under load is not a reliable primitive for the applications built on top of it.

RocksDB via cgo FFI would be correct — RocksDB is battle-tested and used in production by CockroachDB, TiKV, and many others. The reason for the custom Rust implementation: the portfolio goal is to demonstrate mastery of the storage engine internals, not to demonstrate the ability to configure RocksDB. A custom LSM implementation shows understanding of memtable design, SSTable format, bloom filter sizing, compaction strategy selection, and WAL format. Configuring RocksDB shows familiarity with its knobs. These are different skills, and for a portfolio demonstrating distributed systems depth, the custom implementation is the correct choice.

The tradeoff: the custom Rust LSM will have bugs that RocksDB does not have. This is accepted — the bugs are found by the chaos suite and the linearizability checker, which is exactly the purpose of those components.

---

## Chaos Suite: Destructiveness vs. Safety

**Chosen:** Destructive chaos in isolated Docker network, never run against real infrastructure
**Alternative considered:** Fault injection via library hooks, chaos in unit tests

The chaos suite uses actual iptables network manipulation, actual SIGKILL process termination, and actual system clock modification. These are the same mechanisms that cause real failures in production. A chaos suite that simulates failures via mock injection does not test the same code paths that production failures exercise.

The isolation constraint — chaos runs only in an isolated Docker network, never against any real infrastructure — is enforced at the network level, not by convention. The Docker network has no external routes. The chaos orchestrator explicitly verifies it is running in an isolated environment before executing any destructive operation. Running the chaos suite against a real cluster by accident is a network-level impossibility, not just a documented warning.

The Python chaos orchestrator is deliberately separate from the Go node implementation. Chaos must be independent from the system under test — a chaos framework embedded in the node code cannot kill the node process or simulate network partitions at the OS level.

---

## Pre-Vote vs. Standard Raft Election

**Chosen:** Pre-vote extension
**Alternative considered:** Standard Raft election

The pre-vote optimization was described in Ongaro's PhD dissertation (not the original Raft paper). Without it, a partitioned node that repeatedly times out and increments its term can disrupt a stable cluster when it reconnects — the higher term causes the current leader to step down unnecessarily, triggering a re-election.

Pre-vote prevents this by requiring a node to confirm it could win an election before starting one. The node asks peers "would you vote for me?" without incrementing the term. If it cannot get a pre-vote majority, it does not start the election. The partitioned node's disruption is contained.

The tradeoff: pre-vote adds one extra message round-trip to every election. Under normal operation, elections are rare (only when the leader fails). The additional latency per election is negligible compared to the stability benefit.

---

## Summary

| Decision | Chosen | Alternative | Core Reason |
|---|---|---|---|
| Strong consistency | Raft quorum reads (read index) | Leader lease | Read index is correct under any clock behavior; leases require clock bounds |
| Causal consistency | Vector clocks + LWW | HLC, CRDTs | Vector clocks capture causality precisely; LWW covers most use cases |
| Eventual consistency | Async gossip, stale-flagged reads | Bounded staleness | Simpler; staleness is acknowledged, not hidden |
| Storage engine | Custom Rust LSM | RocksDB, BadgerDB | Portfolio goal: demonstrate storage internals, not configuration |
| Chaos suite | Destructive, OS-level, isolated Docker | Mock injection, unit test faults | Same mechanisms as real failures; isolation prevents accidents |
| Election protocol | Pre-vote + standard Raft | Standard Raft only | Pre-vote prevents disruption from partitioned nodes reconnecting |