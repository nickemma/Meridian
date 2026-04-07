# Meridian Architecture

**Status:** In Development
**Last Updated:** April 2026
**Author:** [@nickemma](https://github.com/nickemma)

---

## Overview

Meridian is a distributed key-value store built to prove one thing: that CAP theorem is not an abstract constraint — it is a set of engineering decisions with measurable consequences. Every component in Meridian represents one of those decisions. Raft consensus implements CP — consistency and partition tolerance, at the cost of availability during splits. The eventual consistency path implements AP — availability and partition tolerance, at the cost of stale reads. Causal consistency with vector clocks sits between them — stronger than eventual, weaker than strong, with specific semantics that the client can reason about.

The central design constraint is this: **each consistency level must be implemented correctly, or the system is not useful — it is deceptive.** A system that claims to provide strong consistency but allows dirty reads under partition is worse than a system that makes no consistency guarantee at all. Every component in Meridian is built under the constraint that its consistency semantics must be mathematically verifiable by the chaos suite.

---

## System Map

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client Layer                              │
│        gRPC API  •  Consistency level per request               │
│        Strong  |  Causal  |  Eventual                           │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                    Meridian Node (Go)                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐ │
│  │   Request   │  │  Consistency│  │     Vector Clock        │ │
│  │   Router    │  │  Resolver   │  │     Manager             │ │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                   Raft Consensus Layer (Go)                      │
│  Leader Election  •  Log Replication  •  Commit  •  Snapshot    │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌────────────────────────────────────────────────────────────────┐
│    ┌──────────────────────┐   ┌─────────────────────────┐     │
│    │   Storage Engine      │   │   Replication Layer     │    │
│    │   (Rust)              │   │   (Go)                  │    │
│    │   LSM  WAL  SSTables  │   │   Async gossip          │    │
│    └──────────────────────┘   └─────────────────────────┘     │
└────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                     Cluster (3-5 nodes)                          │
│    Node 1 (Leader)  •  Node 2 (Follower)  •  Node 3 (Follower) │
│              mTLS between all nodes                              │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│              Chaos Orchestrator + Verifier (Python)              │
│   Node kills  •  Partitions  •  Clock skew  •  Linearizability  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Component Responsibilities

### Request Router (Go)

The entry point for all client requests. Inspects the requested consistency level and routes accordingly:

- `STRONG` → Raft consensus layer (all writes and reads go through the leader)
- `CAUSAL` → Vector clock manager + storage (reads return the value at or after the client's vector clock)
- `EVENTUAL` → Local storage read (any node serves directly from its local state)

The router is stateless. It does not make consistency decisions — it delegates to the component responsible for each consistency model.

### Raft Consensus Layer (Go)

Implements the Raft protocol as described in the Ongaro and Ousterhout paper. Three roles: Leader, Follower, Candidate. Three sub-protocols: leader election, log replication, and membership change.

**Leader election** — nodes start as followers. If a follower receives no heartbeat within the election timeout (randomized 150-300ms), it becomes a candidate, increments its term, votes for itself, and sends RequestVote RPCs to all peers. If it receives votes from a majority, it becomes leader and begins sending heartbeats. The randomized timeout prevents split votes from cascading.

**Log replication** — the leader appends all writes to its log as entries, then sends AppendEntries RPCs to all followers. When a majority of nodes (including the leader) have persisted the entry, it is committed and applied to the state machine. The leader sends the commit index to followers in subsequent heartbeats. Followers apply entries up to the commit index.

**Safety invariants enforced:**
- Election safety: at most one leader per term
- Log matching: if two logs have an entry with the same index and term, all preceding entries are identical
- Leader completeness: if an entry is committed in term T, it will appear in the logs of all leaders in terms > T
- State machine safety: if a node applies an entry at a given index, no other node applies a different entry at the same index

### Vector Clock Manager (Go)

Tracks causality for the causal consistency path. Each node maintains a vector clock: an array of logical counters, one per node in the cluster. A write increments the writing node's counter. A read returns both the value and the vector clock at the time of the read. A subsequent causal write includes this clock — the system guarantees that the write is applied after all events that causally precede it.

```
Node 1 writes key "x":    VC = [1, 0, 0]
Node 2 reads key "x":     VC = [1, 0, 0]
Node 2 writes key "y"
  (causally after reading "x"):
                           VC = [1, 1, 0]

Any subsequent read of "y" is guaranteed to see
the state of "x" that Node 2 saw.
```

Conflict detection: two writes are concurrent if neither vector clock dominates the other (neither is ≤ the other componentwise). Concurrent writes are conflicts. Meridian uses last-write-wins (by wall clock, then node ID as tiebreaker) for conflict resolution in v1. The conflict is logged — the resolution is not silent.

### Storage Engine (Rust)

An LSM (Log-Structured Merge) tree implementation providing durable, crash-safe storage. Rust earns its place here for the same reason it earns its place in every write-hot path: deterministic latency with no GC, memory safety without runtime overhead, and the ability to control memory layout precisely for SSTable compaction.

Three components:
- **WAL (Write-Ahead Log)** — every write is appended to the WAL before being applied to the memtable. On crash recovery, the WAL is replayed to reconstruct the memtable.
- **Memtable** — an in-memory sorted map (BTreeMap in Rust). Writes go here after WAL. When the memtable exceeds its size threshold, it is flushed to an SSTable on disk.
- **SSTables** — immutable sorted files on disk. Reads check the memtable first, then SSTables from newest to oldest. Bloom filters on each SSTable skip files that cannot contain the key. Background compaction merges SSTables to bound read amplification.

### Replication Layer (Go)

Handles the eventual consistency path. Async gossip replication between nodes — writes applied locally are propagated to peers in the background without blocking the client. Replication is best-effort: if a node is partitioned, writes buffer up to a configurable limit and are replayed when the partition heals.

The replication layer does not guarantee ordering across nodes in eventual mode. Two writes from different clients to different nodes may be applied in different orders on different replicas. This is the correct behavior for eventual consistency — and it is tested by the chaos suite.

### Chaos Orchestrator (Python)

A test harness that actively destroys the cluster to verify correctness. Runs in an isolated Docker network — never against any real infrastructure. Three categories of chaos:

- **Node kills** — SIGKILL a random node, verify the cluster continues serving requests (if majority remains), verify the killed node rejoins and converges after restart
- **Network partitions** — use Docker network rules (iptables) to isolate nodes or create asymmetric partitions, verify the majority partition continues serving and the minority partition either serves stale reads (eventual) or rejects requests (strong)
- **Clock skew injection** — advance or retard system time on a node, verify vector clock causality is not violated, verify Raft election timeouts handle clock skew correctly

### Linearizability Checker (Python)

A Jepsen-style verification tool. Records every operation (key, operation type, value, timestamp, node) during a chaos run. After the run, verifies the operation history is consistent with a linearizable execution — that there exists a total ordering of all operations that is consistent with real time and the single-value constraint.

The checker uses the Wing and Gong linearizability algorithm: exhaustive search over possible total orderings, pruned by consistency constraints. For small operation histories (hundreds of operations) this is tractable. For large histories, the checker uses the P-compositionality optimization to check per-key histories independently.

---

## Consistency Model Per-Request (End-to-End)

### Strong Consistency Write

```
Client: Put("user:1001", "balance=500", STRONG)
  │
  ↓
Request Router → Consistency Resolver
  └─ Level = STRONG → route to Raft leader
  │
  ↓
Raft Leader
  ├─ Append to local log: (index=42, term=3, key=user:1001, value=balance=500)
  ├─ Send AppendEntries to all followers
  │   ├─ Node 2: AppendEntries ACK
  │   └─ Node 3: AppendEntries ACK
  ├─ Majority received (2 of 2 followers ACKed) → commit index = 42
  ├─ Apply to storage engine (Rust): put(user:1001, balance=500)
  └─ Return success to client
```

### Strong Consistency Read

```
Client: Get("user:1001", STRONG)
  │
  ↓
Raft Leader
  ├─ Read index protocol: record current commit index (42)
  ├─ Send heartbeat to majority of followers to confirm leadership
  │   (confirms this node is still leader — not stale from a partition)
  ├─ Wait for majority heartbeat ACKs
  ├─ Read from storage engine at commit index ≥ 42
  └─ Return value to client
```

The read index protocol is why strong reads go through the leader and still require a network round-trip. Serving reads from local state without the leadership confirmation would allow a partitioned old leader to return stale data — a linearizability violation.

### Causal Consistency Write

```
Client: Put("user:1002", "cart=[item1]", CAUSAL, vc=[1,0,0])
  │
  ↓
Vector Clock Manager
  ├─ Merge client VC [1,0,0] with local VC [0,1,0]
  │   result: [1,1,0] (element-wise max)
  ├─ Increment own component: [1,2,0]
  ├─ Write to local storage with VC [1,2,0]
  └─ Async replicate to peers with VC [1,2,0]
```

### Eventual Consistency Read

```
Client: Get("user:1003", EVENTUAL)
  │
  ↓
Local storage read — no coordination, no quorum
  └─ Return whatever the local node has, including potentially stale data
     (node may be partitioned and behind the leader)
```

---

## Quorum Calculation Under Partial Failure

For a cluster of N nodes, Raft requires a quorum of ⌈N/2⌉ + 1 nodes for any committed operation.

| Cluster Size | Quorum | Max Tolerated Failures |
|---|---|---|
| 3 | 2 | 1 |
| 5 | 3 | 2 |
| 7 | 4 | 3 |

During a partition, the majority partition continues serving strong consistency reads and writes. The minority partition:
- **Strong reads/writes:** rejected — the minority cannot reach quorum
- **Eventual reads:** served from local state — stale but available
- **Causal reads:** served if the client's vector clock is satisfied by local state

This is CAP theorem in operation. Meridian does not hide the partition from clients — it returns explicit error codes on the strong consistency path when quorum is unreachable, and returns stale data with a `STALE_READ` flag on the eventual path. The client knows what kind of data it is getting.

---

## Raft Log Compaction

The Raft log grows unboundedly without compaction. Log compaction in Meridian:

1. The leader takes a snapshot of the current storage engine state at a log index
2. The snapshot is written to disk (Rust storage engine serializes state)
3. All log entries up to the snapshot index are deleted from the Raft log
4. New followers that join after compaction receive the snapshot instead of replaying the full log

Snapshot transfer uses a streaming gRPC call — large snapshots are sent in chunks. The follower applies the snapshot atomically: it discards its current state and replaces it with the snapshot. If the transfer is interrupted, the follower retains its previous state and requests the snapshot again.

---

## Module Structure

```
meridian/
├── cmd/
│   ├── node/             ← Go node server entrypoint
│   └── meridian-cli/     ← Go admin CLI
├── internal/
│   ├── raft/             ← Raft state machine, leader election, log replication
│   ├── router/           ← Consistency level routing
│   ├── vectorclock/      ← Vector clock management, merge, conflict detection
│   ├── replication/      ← Async gossip replication for eventual path
│   ├── quorum/           ← Quorum calculation, partition detection
│   ├── snapshot/         ← Snapshot creation, streaming transfer
│   └── middleware/       ← mTLS, node identity verification, audit log
├── storage/              ← Rust LSM storage engine (WAL, memtable, SSTable)
├── chaos/                ← Python chaos orchestrator
├── checker/              ← Python linearizability verifier
├── proto/                ← Protobuf definitions (client API + inter-node)
├── docker/               ← Multi-node cluster docker-compose
└── docs/
```

---

## References

- [Design Doc](DESIGN_DOC.md) — Raft log replication deep dive, vector clock merge algorithm, quorum under partial failure, chaos suite design
- [Tradeoffs](TRADEOFFS.md) — strong vs causal vs eventual, LSM read amplification, chaos suite destructiveness
- [Runbook](RUNBOOK.md) — split-brain recovery, log divergence repair, snapshot restore
- [Raft paper](https://raft.github.io/raft.pdf) — Ongaro & Ousterhout
- [Vector clocks](https://lamport.azurewebsites.net/pubs/time-clocks.pdf) — Lamport, "Time, Clocks, and the Ordering of Events"
- [Linearizability](https://cs.brown.edu/~mph/HerlihyW90/p463-herlihy.pdf) — Herlihy & Wing