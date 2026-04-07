# Meridian Runbook

**Purpose:** Operational procedures for diagnosing and recovering from failures in a Meridian cluster.
**Status:** Grows with the system — updated as each phase ships.
**Last Updated:** April 2026

---

## Guiding Principle

A distributed KV store that requires distributed systems expertise to recover from failures is not a usable system. Every procedure in this runbook is written to be followed by an operator who understands that the cluster is in a bad state and needs to know the safe path out — not to be followed by someone who designed the system. The most dangerous recovery actions are labeled explicitly. Commands that modify cluster state require confirmation.

---

## Diagnostics

### Cluster health

```bash
./bin/meridian-cli cluster status
# NODE  ROLE      TERM  COMMIT_INDEX  LAST_APPLIED  STATUS
# 1     leader    7     10432         10432         healthy
# 2     follower  7     10432         10432         healthy
# 3     follower  7     10429         10429         lagging  ← replication lag

# Replication lag per node
./bin/meridian-cli cluster lag
# Node 3 lag: 3 entries (12ms behind)
```

### Check Raft log state

```bash
./bin/meridian-cli log status --node 1
# Last log index:   10432
# Commit index:     10432
# Last applied:     10432
# Snapshot index:   9000   (log compacted up to this point)
# Term:             7
```

### Check vector clock state

```bash
./bin/meridian-cli vc status --node 2
# Node 2 vector clock: [10432, 10430, 10429]
# (Node 1: 10432 events seen, Node 2: 10430 events seen, Node 3: 10429 events seen)
```

### Check consistency level distribution

```bash
# Via Prometheus
curl http://node1:9090/metrics | grep meridian_requests_by_consistency
# meridian_requests_by_consistency{level="strong"}  1423
# meridian_requests_by_consistency{level="causal"}  8341
# meridian_requests_by_consistency{level="eventual"} 24891
```

---

## Failure Triage

### No Leader — Cluster Cannot Elect

Symptoms: all nodes report as follower or candidate, no writes accepted, strong reads return QUORUM_UNAVAILABLE.

1. Check the current state of all nodes:
```bash
./bin/meridian-cli cluster status
# If all nodes show CANDIDATE or FOLLOWER with no LEADER, election is failing
```

2. Check the term on each node — is election cycling (term incrementing rapidly)?
```bash
for node in 1 2 3; do
  ./bin/meridian-cli log status --node $node | grep "Term:"
done
# If terms differ significantly or are increasing, election cycling is occurring
```

3. Check network connectivity between nodes:
```bash
# From each node, verify gRPC connectivity to peers
./bin/meridian-cli ping --from 1 --to 2
./bin/meridian-cli ping --from 1 --to 3
./bin/meridian-cli ping --from 2 --to 3
```

4. If network is healthy and election is still failing, check mTLS certificate validity:
```bash
./bin/meridian-cli cert status --node 1
# If certificates have expired, nodes cannot authenticate and will not vote for each other
```

5. If certificates are the issue, rotate them:
```bash
make certs-rotate  # Re-issues all node certificates
# Restart nodes one at a time after certificate rotation
```

6. If network is partitioned (fewer than quorum nodes can communicate), the cluster cannot elect a leader until the partition heals. Wait for the partition to heal — do not attempt to reduce the quorum size, as this can lead to split-brain.

---

### Split-Brain Recovery

Symptoms: two nodes both report as leader (same or different terms). This should not happen in a correct Raft implementation — if observed, it indicates a bug.

1. **Stop all writes immediately.** A split-brain in a correct Raft implementation is a critical bug. Do not attempt to recover without fully understanding what happened.

2. Capture the state of all nodes:
```bash
for node in 1 2 3; do
  ./bin/meridian-cli log dump --node $node > node${node}_log_$(date +%s).json
done
```

3. Determine which node has the higher commit index:
```bash
for node in 1 2 3; do
  ./bin/meridian-cli log status --node $node | grep "Commit index:"
done
```

4. The node with the higher commit index has more committed writes. All other nodes' logs must converge to this node's committed log.

5. Shut down all nodes except the one with the highest commit index:
```bash
./bin/meridian-cli node stop --id 2
./bin/meridian-cli node stop --id 3
```

6. Restart the stopped nodes. They will join as followers and receive log replication from the surviving node (which will be elected leader as the only running node, then accept followers as they rejoin):
```bash
./bin/meridian-cli node start --id 2
./bin/meridian-cli node start --id 3
```

7. Verify convergence:
```bash
./bin/meridian-cli cluster status
# All nodes should show same commit index
```

8. File a bug report with the full log dumps from step 2. Split-brain in Raft is a correctness violation that requires root cause analysis.

---

### Log Divergence Repair

Symptoms: a follower's log diverges from the leader's log and the automatic repair (nextIndex probe and AppendEntries backfill) is not converging.

1. Check the follower's log against the leader's:
```bash
./bin/meridian-cli log diff --leader 1 --follower 3
# Shows the first index where logs diverge and the entries that differ
```

2. The automatic repair should handle this via the Raft nextIndex protocol. Check if it is running:
```bash
./bin/meridian-cli replication status --node 3
# nextIndex[3]: 9842
# matchIndex[3]: 9841
# Last AppendEntries: 2 seconds ago
# Status: converging (replication in progress)
```

3. If replication is stuck (nextIndex not advancing for > 60 seconds):
```bash
# Check if the follower can receive RPCs from the leader
./bin/meridian-cli ping --from 1 --to 3

# Check if the follower has sufficient disk space for incoming entries
df -h /var/lib/meridian/node3
```

4. If the follower's log is corrupted beyond repair by the normal Raft mechanism:

   **⚠️ Destructive operation — verify the follower has no committed writes not present on the leader before proceeding.**

```bash
# Verify: leader commit index should be ≥ follower commit index
./bin/meridian-cli log status --node 1 | grep "Commit index:"
./bin/meridian-cli log status --node 3 | grep "Commit index:"

# If leader commit >= follower commit, it is safe to wipe the follower's log
# The follower will receive a snapshot from the leader on restart

./bin/meridian-cli node stop --id 3
rm -rf /var/lib/meridian/node3/raft-log/
# (do NOT remove the WAL — only the raft log directory)
./bin/meridian-cli node start --id 3
# Node 3 will request a snapshot from the leader and apply it
```

5. Verify convergence after restart:
```bash
./bin/meridian-cli log diff --leader 1 --follower 3
# Should show: logs are identical
```

---

### Snapshot Restore Procedure

A node's storage is corrupted or lost entirely (disk failure, accidental deletion). The node must be restored from a snapshot.

1. Stop the failed node:
```bash
./bin/meridian-cli node stop --id 3
```

2. Clear the node's data directory entirely:
```bash
rm -rf /var/lib/meridian/node3/
mkdir -p /var/lib/meridian/node3/
```

3. Start the node with the `--bootstrap-from-snapshot` flag:
```bash
./bin/meridian-cli node start --id 3 --bootstrap-from-snapshot
# The node connects to the leader, requests the latest snapshot,
# receives it via streaming gRPC (chunked), applies it,
# then receives log entries from the snapshot index onward
```

4. Monitor the snapshot transfer:
```bash
./bin/meridian-cli node status --id 3
# Status: receiving_snapshot (42% complete, 1.2GB / 2.8GB)
```

5. After snapshot application, the node catches up via log replication:
```bash
./bin/meridian-cli replication status --node 3
# Status: converging (log entries 8000-10432 being replicated)
```

6. Verify full recovery:
```bash
./bin/meridian-cli cluster status
# Node 3 should show: healthy, same commit index as leader
```

---

### Causal Read Not Satisfied

A client receives `CAUSAL_NOT_SATISFIED` — the serving node has not yet seen all events that the client's vector clock requires.

This is expected behavior, not an error. The serving node is behind the client's causality frontier.

1. The client library automatically retries with exponential backoff on `CAUSAL_NOT_SATISFIED`. If it is occurring repeatedly (> 5 retries), the replication lag on the serving node may be too high.

2. Check replication lag:
```bash
./bin/meridian-cli cluster lag
# Node 3 lag: 847 entries (3400ms behind)  ← significant lag
```

3. High lag causes frequent `CAUSAL_NOT_SATISFIED`. Check for the root cause:
```bash
# Network throughput to the lagging node
./bin/meridian-cli replication stats --node 3
# bytes_per_second: 1.2MB/s  (normal)
# entries_per_second: 150   (normal)
# queue_depth: 3200          (high — backlog)
```

4. If the backlog is growing faster than it is being consumed, the node is under resource pressure:
```bash
# Check CPU and disk I/O on Node 3
./bin/meridian-cli node metrics --id 3
```

5. For immediate relief, route causal reads away from the lagging node using the client's node hint parameter:
```bash
meridian-cli get --key x --consistency causal --prefer-nodes 1,2
```

---

## Running the Chaos Suite

The chaos suite is destructive. It must only run in the isolated Docker network.

```bash
# Verify isolated network (will fail if not in chaos network)
make chaos-verify

# Run full chaos suite (30 minutes)
make chaos-run

# Run a specific scenario
./chaos/run.py --scenario node_kill_leader --duration 5m

# Run linearizability check on captured history
./checker/verify.py --history chaos-run-2026-04-06.json
```

If the linearizability checker fails, it produces a counterexample: a specific ordering of operations that cannot be explained by a linearizable execution. This is a correctness bug. Do not dismiss it, do not rerun hoping for a different result. File the counterexample, the cluster configuration, and the full chaos scenario log as a critical bug.

---

## Environment Variables Reference

```bash
MERIDIAN_NODE_ID              # This node's ID (integer, unique in cluster)
MERIDIAN_PEERS                # Comma-separated peer addresses (host:port)
MERIDIAN_RAFT_PORT            # Port for inter-node Raft RPCs
MERIDIAN_CLIENT_PORT          # Port for client gRPC connections
MERIDIAN_DATA_DIR             # Data directory for WAL, SSTables, Raft log
MERIDIAN_ELECTION_TIMEOUT_MS  # Randomized upper bound for election timeout (default: 300)
MERIDIAN_HEARTBEAT_INTERVAL_MS # Leader heartbeat interval (default: 50)
MERIDIAN_QUORUM_SIZE          # Required quorum size (default: ⌈N/2⌉ + 1)
MERIDIAN_WAL_ENCRYPTION_KEY   # AES-256-GCM key for WAL encryption (32 bytes)
MERIDIAN_MEMTABLE_SIZE_MB     # Memtable flush threshold (default: 64)
MERIDIAN_SNAPSHOT_INTERVAL    # Log entries between snapshots (default: 10000)
MERIDIAN_REPLICATION_BUFFER   # Max buffered eventual writes during partition (default: 10000)
MERIDIAN_LOG_LEVEL            # debug | info | warn | error (default: info)
```

---

## See Also

- [Architecture](ARCHITECTURE.md) — system map, consistency models end-to-end, quorum under partial failure
- [Design Doc](DESIGN_DOC.md) — Raft log replication, vector clock algorithm, chaos suite design, LSM storage engine
- [Tradeoffs](TRADEOFFS.md) — strong vs causal vs eventual, Rust LSM vs RocksDB, pre-vote extension