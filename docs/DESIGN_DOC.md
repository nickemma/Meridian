# Meridian Design Document

**Status:** In Development
**Last Updated:** April 2026
**Author:** [@nickemma](https://github.com/nickemma)

---

## Purpose

This document explains the hard problems in Meridian — the places where distributed systems theory becomes distributed systems engineering. It covers the Raft log replication protocol in implementation detail, the vector clock merge algorithm and why it handles concurrency correctly, quorum calculation under partial failure (including the non-obvious cases), and the chaos suite design. It also covers the problems that are unique to the combined system: the secret rotation protocol and why it requires a single atomic log entry, the WASM policy evaluation sandbox and its fuel-based termination model, and the audit log hash chain and how it survives snapshot compaction.

These are not descriptions of how things work in principle. They are descriptions of how Meridian implements them and where the implementation decisions diverge from the naive reading of the underlying papers.

---

## Raft Log Replication Deep Dive

### What the paper says vs. what you actually implement

The Raft paper is the clearest consensus protocol paper written. It is still not a complete implementation specification. It describes the algorithm and its safety invariants. It does not describe the practical decisions that every implementation must make: how large should AppendEntries batches be, what happens when a follower's log has entries beyond the leader's commit index, how do you handle the case where a candidate wins an election with a log behind the previous leader's committed entries.

Meridian's Raft implementation makes these decisions explicitly.

### AppendEntries batching

Sending one AppendEntries RPC per log entry serializes all replication. Instead, the leader batches all pending log entries (up to `MERIDIAN_RAFT_MAX_BATCH_ENTRIES`, default 100) into a single RPC per peer. Each peer has a dedicated replication goroutine. Goroutines are independent — a slow follower does not block replication to a fast follower. The leader tracks `nextIndex` and `matchIndex` per peer independently.

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

### Log divergence after leader failure

When a new leader is elected, followers may have log entries the new leader does not have — appended by the old leader but never committed before it was partitioned. The new leader handles this through the `nextIndex` probe:

```
New leader elected
  │
  ├─ Initialize nextIndex[peer] = leader.lastLogIndex + 1
  │
  └─ AppendEntries to peer fails (log mismatch)
      │
      └─ Decrement nextIndex[peer]
          │
          └─ Retry at lower index
              │
              └─ Repeat until log matches at nextIndex - 1
                  │
                  └─ Send all entries from nextIndex onward
                     (overwrites peer's uncommitted entries)
```

The safety guarantee: entries can only be overwritten if they were never committed. Raft's election restriction ensures the new leader's log contains all committed entries.

### The election restriction

A node votes for a candidate only if the candidate's log is at least as up-to-date as its own:

1. If the logs have different last terms, the log with the higher last term wins
2. If the logs have the same last term, the longer log wins

This is the single most important safety property in Raft. It is easy to implement incorrectly by checking only the log length and not the term of the last entry.

### Pre-vote optimization

Meridian implements the pre-vote extension from the Raft dissertation. Without it, a partitioned node that repeatedly times out accumulates a high term. When it reconnects, it causes the current leader to step down unnecessarily — a disruption to a stable cluster.

Pre-vote adds a phase before the candidate increments the term: it asks peers "would you vote for me?" without starting a real election. If it cannot get a quorum of pre-votes, it does not start the election. The disruption is contained.

---

## Secret Rotation Protocol

### The problem: two-step writes under partition

A naive rotation implementation makes two sequential writes: write the new secret version, then update the `current` pointer to point to it. If the leader crashes between these two writes, the cluster ends up with a new version in storage but the `current` pointer still pointing to the old version. When the cluster recovers, the new version exists but is unreachable without knowing its version identifier.

Worse: if the `current` pointer update reaches a quorum but the new version write does not, the pointer refers to a version that does not exist on all nodes.

### The solution: atomic log entry

The rotation protocol packages both writes — the new version entry and the `current` pointer update — into a single Raft log entry:

```
Rotation log entry (index=1042, term=7):
  writes:
    - key: secret:services/payments/db-password:v5
      value: {ciphertext, created_at, ttl, rotation_schedule}
    - key: secret:services/payments/db-password:current
      value: {version: "v5", valid_until: <grace_period_end>}
    - key: secret:services/payments/db-password:v4:revocation
      value: {scheduled_at: <now>, effective_at: <grace_period_end>}
```

Either all three writes are applied — committed to a quorum and applied to the state machine — or none are. There is no intermediate state visible to any reader.

### Grace period and revocation

When a rotation commits, the old version is not immediately revoked. Services holding leases issued against the old version have a grace period (default 5 minutes, configurable) to renew their lease against the new version. After the grace period, the revocation event is a committed Raft entry — the old version is rejected cluster-wide, on every node, at the same committed index.

A service holding an expired lease for the old version after the grace period cannot read the old credential from any node — not from the leader, not from a follower, not from a node that missed the revocation event (such a node is behind the commit index and will apply the revocation before serving any eventual reads, because lease expiry validation checks the local commit state before serving even eventual reads).

### Rotation under leader failover

If the leader dies mid-rotation, the log entry is either committed or not:

- **Not committed (did not reach quorum before crash):** the new leader is elected from the majority. The rotation entry is absent from the majority log. Raft's log divergence repair overwrites the entry on any follower that received it from the old leader. The rotation did not happen. The client receives an error and retries.
- **Committed (reached quorum before crash):** the entry is in the majority log. The new leader applies it as part of log replay. The rotation completed correctly. The client may not have received the success response (the old leader crashed before responding) — a retry produces an idempotent result because the version identifier is deterministic.

The chaos suite specifically tests both paths.

---

## Vector Clock Merge Algorithm

### The correctness requirement

Causal consistency guarantees: if operation A causally precedes operation B (A → B), any node that has seen B must also have seen A. Vector clocks encode this relationship: V(A) < V(B) componentwise means A causally precedes B.

In Meridian, causal consistency is used for lease renewals and policy reads — operations where the client needs a causally consistent view of prior writes, without paying for quorum.

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
  VC_i[j] += 1
  apply the write

Causality check (is A causally before B?):
  A → B iff VC_A[k] ≤ VC_B[k] for all k
          AND VC_A[k] < VC_B[k] for at least one k

Concurrent writes (neither precedes the other):
  VC_A[j] > VC_B[j] for some j
  AND VC_B[k] > VC_A[k] for some k
```

### Conflict resolution

Concurrent writes to the same key are resolved via last-write-wins: later wall clock timestamp wins, with node ID as tiebreaker on equal timestamps. Every conflict is logged — the two concurrent writes, the winning write, and the resolution reason. Conflicts are never silent.

### Causal read protocol

```
Client: GetLease(lease_id, CAUSAL, client_vc=[4,3,2])

At the serving node:
  ├─ Is local_vc ≥ client_vc componentwise?
  │   ├─ Yes → read from local storage, return value + local_vc
  │   └─ No  → this node hasn't seen all events the client has seen
  │            └─ Return CAUSAL_NOT_SATISFIED
  │               Client retries with exponential backoff
```

---

## Quorum Calculation Under Partial Failure

### The standard case

For N=3: quorum=2. One node can fail. For N=5: quorum=3. Two nodes can fail.

### Asymmetric partition

```
3-node cluster: Node1, Node2, Node3

Partition: Node2 → Node3: BLOCKED (asymmetric, one direction)

Node1 can reach both Node2 and Node3 → Node1 can be leader (quorum exists)
Node2 can reach Node1 → can reach quorum with Node1
Node3 can reach Node1 → can reach quorum with Node1

Result: Node1 bridges both sides. No split. Correct behavior.
```

### Split-brain protection

```
5-node cluster: Partition A (Node1, Node2), Partition B (Node3, Node4, Node5)

Partition B: 3 nodes — can form quorum → elects new leader (higher term)
Partition A: 2 nodes — cannot form quorum → cannot elect leader

The original leader (in Partition A) cannot get heartbeat ACKs from a majority.
It steps down. There is no scenario where two leaders coexist in the same term.
```

The step-down is the key: Meridian's leader requires majority heartbeat ACKs on an ongoing basis. A partitioned leader that cannot reach the majority steps down within one election timeout. It does not continue accepting secret writes. Any writes it accepted before stepping down that did not reach quorum are not committed.

### What partition means for secrets specifically

A service trying to fetch a credential from the minority partition on the strong path receives `QUORUM_UNAVAILABLE`. This is the correct answer. A secrets manager that returns a potentially stale or incorrect credential during a partition is more dangerous than one that returns an error. The error is actionable — retry against a node in the majority. The wrong credential is silent data corruption.

A service fetching on the eventual path gets the most recent locally-known credential version with `STALE_READ: true`. This is appropriate for cached reads where the service already holds the credential and is refreshing proactively.

---

## WASM Policy Evaluation Sandbox

### Why WASM

Policy evaluation is on the critical path — every secret read and write goes through it. The policy runtime must be fast, isolated, and unable to affect the node process regardless of policy content.

In-process evaluation (a Go interpreter or reflection-based rule engine) is fast but not isolated. A policy with an infinite loop, a panic-inducing expression, or memory-hungry data structures can degrade or crash the node. For a platform that runs beneath everything else, this is unacceptable.

WASM provides OS-process-level isolation without OS-process-level overhead. A WASM module that panics cannot affect the host. A WASM module that loops indefinitely is terminated when it exhausts its fuel budget. A WASM module that allocates excessive memory is bounded by the WASM linear memory limit.

### The fuel model

Wasmtime's fuel model assigns a budget of computational instructions to each WASM invocation. When the budget is exhausted, the WASM instance is terminated and the evaluation returns `POLICY_TIMEOUT`. The fuel budget is calibrated to the target p99 evaluation latency (2ms) — a policy that exceeds the budget by 10x cannot use more than 10x the target time.

The fuel model is preferable to a goroutine-based timer because it is deterministic and cannot be bypassed by a WASM module that avoids yielding to the Go scheduler.

### Policy compilation pipeline

```
Policy source (Rego-inspired DSL)
  │
  ↓
Parser → AST
  │
  ↓
Type checker (catches type errors before deployment)
  │
  ↓
WASM compiler (DSL → WASM bytecode)
  │
  ↓
Validation (WASM module is valid, memory limits are set, no forbidden imports)
  │
  ↓
Storage (policy KV entry committed through Raft)
  │
  ↓
Distribution (policy propagated to all nodes via Raft log replication)
```

All nodes evaluate from the same compiled WASM bytecode stored in the Raft log. Policy is not compiled on each node separately — the compiled artifact is the authoritative version. A policy update that fails compilation is rejected before it reaches the Raft log.

### Policy evaluation under partition

During a partition, the minority partition evaluates policy against its local policy version. A policy update committed to the majority partition during the partition is not visible to the minority until healing. This is the correct behavior — the minority's policy version is stale, but it is the most recent version the minority has confirmed as committed. After healing, the minority receives the policy update via log replication and applies it.

The partition does not cause a policy version split where different nodes permanently disagree on policy — Raft's log replication guarantees convergence after healing.

---

## Audit Log Hash Chain

### Structure

Each audit record contains a hash of the previous record:

```
Record 0 (genesis):
  prev_hash: "0000...0000" (null genesis hash)
  hash:      sha256(record_0_content || prev_hash)

Record 1:
  prev_hash: sha256(record_0)
  hash:      sha256(record_1_content || prev_hash)

Record N:
  prev_hash: sha256(record_N-1)
  hash:      sha256(record_N_content || prev_hash)
```

To tamper with Record K, an attacker must recompute the hashes for all records from K through the current record. The current record's hash is committed through Raft and known to all nodes. Any recomputation produces a different hash for the current record — the tampering is detectable by any node.

### Survival through snapshot compaction

When the Raft log is compacted into a snapshot, the audit records up to the snapshot index are included in the snapshot. The snapshot contains the full audit record sequence (not just the final hash) — an auditor can recompute the chain from genesis through the snapshot.

The snapshot's audit sequence ends with a "snapshot boundary" record whose hash becomes the `prev_hash` for the first post-snapshot audit record. The chain is continuous across snapshot boundaries.

```
Records 0-9000: in snapshot
  Snapshot boundary record:
    index:     9000
    prev_hash: sha256(record_9000)
    hash:      sha256(snapshot_boundary || prev_hash)

Record 9001 (post-snapshot):
  prev_hash: sha256(snapshot_boundary)
  hash:      sha256(record_9001_content || prev_hash)
```

Verification runs in two phases: verify the in-snapshot chain from genesis to the snapshot boundary, then verify the post-snapshot chain from the boundary to the current record.

---

## Chaos Suite Design

### Design principle

The chaos suite is an adversarial agent that continuously destroys the cluster while the linearizability checker verifies that the system's behavior is consistent with its stated guarantees. It passes when correctness is provably maintained under the tested failure scenarios. It fails when the system produces a history that cannot be explained by a valid linearizable execution.

In the combined system, the chaos suite adds scenarios that no generic KV store chaos suite covers: secret access under partition, rotation under leader failover, policy enforcement under partition, and anomaly detection under access pattern injection.

### Chaos scenarios

**Node kill (SIGKILL):**
- Start 3-node cluster, run secret reads and writes simultaneously
- SIGKILL a random follower — verify cluster continues serving
- Restart the killed node — verify it rejoins, catches up, and serves correctly
- SIGKILL the leader — verify new leader elected within 5 seconds, no committed writes lost

**Network partition (iptables) — core:**
- Partition 2+1; verify majority serves strong writes, minority rejects strong reads with `QUORUM_UNAVAILABLE`
- Verify minority serves eventual reads with `STALE_READ: true`
- Heal partition; verify minority converges; run linearizability checker

**Secret access under partition:**
- Service holds a valid lease for credential version v4
- Partition cluster 2+1; service is on the minority side
- Service attempts strong read — receives `QUORUM_UNAVAILABLE` (correct)
- Service attempts eventual read — receives v4 with `STALE_READ: true` (correct)
- Majority rotates the credential to v5 during partition
- Heal partition; verify minority applies the rotation; verify the service's next strong read returns v5; verify the v4 lease is now expired (past grace period)

**Rotation under leader failover:**
- Begin rotation of a credential (single atomic log entry in flight)
- SIGKILL the leader before the AppendEntries RPC reaches quorum
- Verify new leader is elected; verify rotation is absent from majority log
- Verify client receives error; retry rotation — succeeds on second attempt
- Repeat with the leader killed after AppendEntries reaches quorum — verify rotation is committed correctly on new leader

**Policy enforcement under partition:**
- Upload a policy that allows access only from 10.0.0.0/8
- Partition cluster; update policy to also allow 192.168.0.0/16 on majority
- Verify minority still evaluates old policy (correct — it has not seen the update)
- Heal partition — verify minority applies updated policy via log replication
- Verify a 192.168.x.x source is denied on minority before healing, allowed after

**Anomaly injection:**
- Service `payments-service` establishes baseline over 100 accesses (normal hours, normal IPs)
- Inject access from a new IP CIDR not in the baseline — verify anomaly score exceeds threshold
- In enforce mode: verify the access is denied and the denial is in the audit log with the score
- In alert mode: verify the access is allowed, the score is emitted as a Prometheus metric, the audit record includes the score

**Clock skew injection:**
- Advance system time on a follower by +5 minutes
- Verify vector clock causality is not violated (vector clocks are logical — clock skew does not affect them)
- Verify LWW conflict resolution with skewed wall clock is logged with an anomaly note
- Retard leader clock by -2 minutes — verify Raft election stability is not affected

### Linearizability checker algorithm

The checker collects an operation history H: a sequence of (key, op, value, start_time, end_time, node) tuples. It verifies H is linearizable by checking whether a sequential history S exists that:

1. Contains exactly the operations in H
2. Preserves real-time ordering (if op A finishes before op B starts, A precedes B in S)
3. Is consistent with the KV store specification (each read returns the value of the most recent preceding write for that key in S)

The Wing-Gong algorithm with P-compositionality optimization runs per-key histories in parallel. The chaos suite limits concurrency to keep the search tractable. The 30-minute full battery run is the exit criterion for Phase 8.

---

## LSM Storage Engine Design (Rust)

### Write path

```
Write arrives
  │
  ├─ Append to WAL (AES-256-GCM, fsync — durability guarantee)
  ├─ Insert into memtable (BTreeMap — in-memory sorted)
  │
  └─ If memtable size > threshold (default 64MB):
      ├─ Freeze current memtable (immutable)
      ├─ Start new empty memtable
      └─ Flush frozen memtable to SSTable (background goroutine)
```

### Read path

```
Read arrives
  │
  ├─ Check memtable (most recent writes, O(log n))
  ├─ If not found: check each SSTable, newest to oldest
  │   ├─ Check bloom filter — key possibly in this SSTable?
  │   │   └─ No → skip (no disk read)
  │   └─ Yes → binary search SSTable index, read data block
  │
  └─ Return first found value (newest SSTable wins)
```

Target read amplification: ≤ 4 SSTables per read after compaction.

### Compaction strategy

Meridian uses leveled compaction. SSTables are organized into levels (L0, L1, L2, ...). L0 is written by memtable flushes. When L0 reaches a threshold, files are compacted into L1. When L1 reaches size, a file is merged with overlapping L2 files, and so on.

Leveled compaction bounds read amplification (at most one file per level needs to be read for any key) at the cost of higher write amplification. For a secrets platform where reads are on the critical path, bounded read amplification is the correct trade.

### WAL encryption

Each WAL record is encrypted separately with AES-256-GCM. The key is provided at node startup via `MERIDIAN_WAL_ENCRYPTION_KEY` and is never stored on disk. The encryption overhead is per-record, not per-byte — bounded by write count, not write volume.

---

## References

- [Architecture](ARCHITECTURE.md) — system map, component responsibilities, end-to-end consistency flows
- [Tradeoffs](TRADEOFFS.md) — strong vs causal vs eventual, WASM sandbox vs in-process, ML anomaly detection model choice
- [Runbook](RUNBOOK.md) — operational procedures for every failure mode described here
- [Raft paper](https://raft.github.io/raft.pdf) — Ongaro & Ousterhout
- [Vector clocks](https://lamport.azurewebsites.net/pubs/time-clocks.pdf) — Lamport
- [Linearizability](https://cs.brown.edu/~mph/HerlihyW90/p463-herlihy.pdf) — Herlihy & Wing
- [WebAssembly spec](https://webassembly.github.io/spec/) — for the WASM sandbox implementation
- [Wasmtime](https://wasmtime.dev/) — the WASM runtime used for policy evaluation
