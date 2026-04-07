# Meridian Design Document

**Status:** In Development
**Last Updated:** April 2026
**Author:** [@nickemma](https://github.com/nickemma)

---

## Purpose

This document explains the hard problems in Meridian — the places where distributed systems theory becomes distributed systems engineering. It covers the Raft log replication protocol in implementation detail, the vector clock merge algorithm and why it handles concurrency correctly, quorum calculation under partial failure (including the non-obvious cases), and the chaos suite design. These are not descriptions of how Raft works in principle. They are descriptions of how Meridian implements it and where the implementation decisions diverge from the naive reading of the paper.

---

## Raft Log Replication Deep Dive

### What the paper says vs. what you actually implement

The Raft paper is the clearest consensus protocol paper written. It is still not a complete implementation specification. The paper describes the algorithm and its safety invariants. It does not describe the practical decisions that every implementation must make: how large should AppendEntries batches be, what happens when a follower's log has entries beyond the leader's commit index, how do you handle the case where a candidate wins an election with a log that is behind the previous leader's committed entries.

Meridian's Raft implementation makes these decisions explicitly.

### AppendEntries batching

Sending one AppendEntries RPC per log entry would serialize all replication — the leader waits for each RPC to complete before sending the next. Instead, the leader batches all pending log entries (up to a configurable maximum) into a single AppendEntries RPC per peer. The batch size is bounded by `MERIDIAN_RAFT_MAX_BATCH_ENTRIES` (default 100 entries) to prevent large RPCs from blocking the network.

```
Write arrives at leader
  │
  └─ Append to local log
      │
      └─ Signal replication goroutine (one per peer)
          │
          └─ Batch all pending entries (up to max batch size)
              │
              └─ Send AppendEntries(entries=[...]) to peer
```

Each peer has a dedicated replication goroutine. Goroutines are independent — a slow follower does not block replication to a fast follower. The leader tracks the `nextIndex` and `matchIndex` per peer independently.

### Log divergence after leader failure

The subtlest correctness property in Raft: when a new leader is elected, followers may have log entries that the new leader does not have. These entries were appended by the old leader but never committed (the old leader was partitioned before they reached quorum).

The new leader handles this through the `nextIndex` probe:

```
New leader elected
  │
  ├─ Initialize nextIndex[peer] = leader.lastLogIndex + 1 for all peers
  │
  └─ AppendEntries to peer fails (log mismatch at nextIndex - 1)
      │
      └─ Decrement nextIndex[peer]
          │
          └─ Retry AppendEntries at lower index
              │
              └─ Repeat until peer's log matches at nextIndex - 1
                  │
                  └─ Send all entries from nextIndex[peer] onward
                      (this overwrites the peer's uncommitted entries)
```

The safety guarantee: entries can only be overwritten if they were never committed. Raft's election restriction (a candidate cannot win an election unless its log is at least as up-to-date as any committed entry in the cluster) ensures the new leader's log contains all committed entries.

### The election restriction (election safety in practice)

A node votes for a candidate only if the candidate's log is at least as up-to-date as its own. "At least as up-to-date" is defined by the paper as:

1. If the logs have different last terms, the log with the higher last term is more up-to-date
2. If the logs have the same last term, the longer log is more up-to-date

This restriction is what prevents a candidate with a stale log from becoming leader and overwriting committed entries. It is the single most important safety property in Raft, and it is easy to implement incorrectly by checking only the log length and not the term of the last entry.

### Pre-vote optimization

Meridian implements the pre-vote extension from the Raft dissertation. Without pre-vote, a node that was partitioned for an extended period accumulates a high term from repeated failed elections. When it reconnects, it disrupts the stable cluster with a higher term — causing the current leader to step down unnecessarily.

Pre-vote adds a phase before a candidate actually starts an election: it asks peers if they would vote for it in a real election without incrementing the term. If it cannot get a quorum of pre-votes, it does not start the election. The partitioned node's disruption is contained.

---

## Vector Clock Merge Algorithm

### The correctness requirement

A causal consistency guarantee means: if operation A causally precedes operation B (A → B), then any node that has seen B must also have seen A. Vector clocks encode causality: V(A) < V(B) (A's clock is dominated by B's clock componentwise) means A causally precedes B.

The merge algorithm must preserve this property across concurrent writes from different nodes.

### The algorithm

```
Each node i maintains: VC_i = [v_1, v_2, ..., v_n]
  where v_j = number of events from node j that node i has seen

On local write at node i:
  VC_i[i] += 1
  attach VC_i to the write

On receiving a write with clock VC_recv from node j:
  for each k in 1..n:
    VC_i[k] = max(VC_i[k], VC_recv[k])
  VC_i[j] += 1  (acknowledge the received event)
  apply the write

Causality check (is A causally before B?):
  A → B if and only if:
    VC_A[k] ≤ VC_B[k] for all k, AND
    VC_A[k] < VC_B[k] for at least one k

Concurrent writes (neither precedes the other):
  NOT (A → B) AND NOT (B → A)
  i.e., VC_A[j] > VC_B[j] for some j AND VC_B[k] > VC_A[k] for some k
```

### Conflict resolution for concurrent writes

When two writes to the same key are concurrent (neither vector clock dominates the other), both are valid from a causality standpoint. Meridian must apply one. The resolution policy in v1:

1. Compare wall clock timestamps — later timestamp wins
2. If timestamps are equal (within 1ms), the write from the higher node ID wins

This is last-write-wins (LWW). It is the correct choice for a portfolio project and for most practical use cases. It is not the correct choice for all use cases — a shopping cart that needs to merge concurrent additions rather than overwrite them would need CRDTs (Conflict-free Replicated Data Types). CRDTs are a v2 concern. LWW is documented, not hidden.

Every conflict is logged: the two concurrent writes, the winning write, and the resolution reason. The conflict log is queryable. Operators can audit how often concurrent writes occur and whether LWW is producing correct results for their use case.

### Causal read protocol

```
Client requests Get("x", CAUSAL, client_vc=[2,1,0])

At the serving node:
  ├─ Check local VC: is local_vc ≥ client_vc componentwise?
  │   ├─ Yes → read from local storage, return value + local_vc
  │   └─ No  → this node hasn't seen all events the client has seen
  │            └─ Option 1: wait for replication to catch up (bounded wait)
  │               Option 2: return CAUSAL_NOT_SATISFIED error
  │               Meridian uses Option 2 with a bounded retry in the client library
```

The causal read guarantee: if a client previously wrote or read at VC=[2,1,0], any subsequent causal read will see a state that includes at least those events. The client's vector clock is the causality token — it is the client's responsibility to pass it between reads and writes.

---

## Quorum Calculation Under Partial Failure

### The standard case

For N=3: quorum = 2. One node can fail. Two nodes continue serving strong reads and writes.

For N=5: quorum = 3. Two nodes can fail. Three nodes continue serving.

This is well-understood. The non-obvious cases are the ones the chaos suite tests.

### Asymmetric partition

```
3-node cluster: Node1, Node2, Node3

Partition: Node1 ↔ Node2 can communicate
           Node1 ↔ Node3 can communicate
           Node2 → Node3 BLOCKED (asymmetric)

From Node1's perspective: can reach Node2 AND Node3 → quorum exists, Node1 can be leader
From Node2's perspective: can reach Node1, cannot reach Node3 → can reach quorum with Node1
From Node3's perspective: can reach Node1, cannot reach Node2 → can reach quorum with Node1

Result: Node1 remains leader (or any node with visibility to majority)
        The partition does not cause a split — Node1 bridges both sides

This is correct behavior. Raft's majority quorum handles asymmetric partitions.
```

### Split-brain scenario (the dangerous case)

```
5-node cluster partitioned into two groups:
  Partition A: Node1, Node2 (2 nodes — cannot form quorum)
  Partition B: Node3, Node4, Node5 (3 nodes — can form quorum)

Partition B elects a new leader (higher term) — correct
Partition A cannot elect a leader (no quorum) — correct

BUT: if the original leader was in Partition A and does NOT know it's partitioned,
     it may continue accepting writes from clients it can still reach.

Meridian's protection: the leader's heartbeat round-trips to a majority of peers.
If the leader cannot get heartbeat ACKs from a majority, it steps down.
There is no scenario where Meridian allows two leaders to coexist in the same term.
```

### The false quorum risk

In a 5-node cluster with 2 simultaneous node failures (tolerated), if the remaining 3 nodes are asymmetrically partitioned such that no single node can reach the other 2:

```
Node3 → Node4: BLOCKED
Node3 → Node5: BLOCKED
Node4 → Node5: BLOCKED
(Each node can only reach itself — no quorum possible)

Result: cluster becomes fully unavailable for strong writes
        Eventual reads continue from local state on each node

This is correct. 3 simultaneous failures exceed the cluster's fault tolerance.
Meridian reports QUORUM_UNAVAILABLE, not a silently wrong answer.
```

---

## Chaos Suite Design

### Design principle

The chaos suite is not a test that runs once to check if the system starts up. It is an adversarial agent that continuously destroys the cluster while the linearizability checker verifies that the system's behavior under destruction is still consistent with its stated guarantees. The chaos suite passes when the system is provably correct under the tested failure scenarios. It fails when the system produces a history that cannot be explained by a valid linearizable execution.

### Chaos scenarios implemented

**Node kill (SIGKILL):**
1. Start 3-node cluster, run writes on all nodes simultaneously
2. SIGKILL a random follower
3. Verify the remaining 2 nodes continue serving (majority intact)
4. Restart the killed node
5. Verify it rejoins, catches up via log replication, and serves correctly
6. SIGKILL the leader
7. Verify a new leader is elected within 5 seconds
8. Verify no committed writes are lost after the leader restart

**Network partition (iptables):**
1. Run concurrent writes from clients to all nodes
2. Partition the cluster 2+1 (majority + minority)
3. Verify: majority partition continues strong writes, minority rejects strong writes
4. Verify: minority partition serves stale eventual reads, flagged as stale
5. Heal the partition
6. Verify the minority node converges to the majority's state
7. Run the linearizability checker on the full operation history

**Clock skew injection:**
1. Advance system time on a follower by +5 minutes
2. Verify vector clock causality is not violated (vector clocks are logical, not physical — but physical timestamps used for LWW conflict resolution must be handled correctly)
3. Verify Raft election timeouts are not prematurely triggered by clock skew
4. Retard system time on the leader by -2 minutes
5. Verify the leader does not step down due to perceived election timeout

**Concurrent write storm:**
1. Fire 1000 concurrent writes to random keys from 10 clients simultaneously
2. Half writes to the leader (strong consistency), half to followers (eventual consistency)
3. After writes complete, query all keys from all nodes
4. Verify strong-consistency writes are present on all nodes
5. Verify eventual-consistency writes converged within the replication window
6. Run the linearizability checker on the strong-consistency writes

### Linearizability checker algorithm

The checker collects an operation history H: a sequence of (key, op, value, start_time, end_time, node) tuples. It verifies H is linearizable by checking whether there exists a sequential history S that:

1. Contains exactly the operations in H
2. Preserves the real-time ordering of H (if op A finishes before op B starts, A appears before B in S)
3. Is consistent with the KV store specification (each read returns the value of the most recent preceding write for that key in S)

The search uses the Wing-Gong algorithm with early termination when a valid S is found. For concurrent operations (overlapping time ranges), the checker enumerates possible orderings. The search is bounded by the number of concurrent operations — the chaos suite limits concurrency to keep the checker tractable.

---

## LSM Storage Engine Design (Rust)

### Write path

```
Write arrives
  │
  ├─ Append to WAL (fsync — durability guarantee)
  ├─ Insert into memtable (BTreeMap — in-memory sorted)
  │
  └─ If memtable size > threshold (default 64MB):
      ├─ Freeze current memtable (immutable)
      ├─ Start new empty memtable
      └─ Flush frozen memtable to SSTable on disk (background goroutine)
```

### Read path

```
Read arrives
  │
  ├─ Check memtable (most recent writes, O(log n))
  ├─ If not found: check each SSTable, newest to oldest
  │   ├─ Check bloom filter: is the key possibly in this SSTable?
  │   │   └─ No → skip (no disk read)
  │   └─ Yes → binary search SSTable index, read data block
  │
  └─ Return first found value (newest SSTable wins)
```

Read amplification: in the worst case, a read checks N SSTables before finding the key (or determining it does not exist). Compaction bounds this by merging old SSTables into fewer, larger ones. The target read amplification is ≤ 4 SSTables per read after compaction.

### Compaction strategy

Meridian uses leveled compaction (same as RocksDB default). SSTables are organized into levels (L0, L1, L2, ...). L0 is written by memtable flushes. When L0 reaches a threshold number of files, they are compacted into L1. When L1 reaches a size threshold, a file is picked and merged with overlapping files in L2. And so on.

Leveled compaction bounds read amplification (each level has sorted, non-overlapping key ranges — at most one file per level needs to be read for any key) at the cost of higher write amplification (files are rewritten multiple times as they move through levels). For a KV store where reads matter as much as writes, leveled compaction is the correct choice.

### WAL encryption

The WAL is encrypted using AES-256-GCM with a key from `MERIDIAN_WAL_ENCRYPTION_KEY`. Each WAL record is a separately encrypted block — the encryption overhead is per-record, not per-byte, so the cost is bounded by the number of writes, not their size. The encryption key is never stored on disk — it is provided at node startup.