# Meridian Runbook

**Purpose:** Operational procedures for diagnosing and recovering from failures in a Meridian cluster — including failures in the secrets, policy, and audit layers.
**Status:** Grows with the system — updated as each phase ships.
**Last Updated:** April 2026

---

## Guiding Principle

A platform that runs beneath everything else must be operable by someone who did not build it. Every procedure here is written to be followed by an operator who knows the cluster is in a bad state and needs the safe path out — not by the original author. The most dangerous recovery actions are labeled explicitly. Commands that modify cluster state require confirmation flags.

Two additional principles for a secrets platform specifically:

**Prefer unavailability over incorrectness.** A credential that is unavailable causes a service restart or an alert. A credential that is silently wrong causes a security incident. When in doubt, the safe recovery path is the one that returns an error, not the one that returns data.

**The audit log is the source of truth for what happened.** Before any recovery action, query the audit log for the affected secret path. The audit log tells you what was accessed, by whom, when, and what policy applied. Recovery without reading the audit log is recovery without context.

---

## Diagnostics

### Cluster health

```bash
./bin/meridian-cli cluster status
# NODE  ROLE      TERM  COMMIT_INDEX  LAST_APPLIED  STATUS
# 1     leader    7     10432         10432         healthy
# 2     follower  7     10432         10432         healthy
# 3     follower  7     10429         10429         lagging  ← replication lag

./bin/meridian-cli cluster lag
# Node 3 lag: 3 entries (12ms behind)
```

### Check Raft log state

```bash
./bin/meridian-cli log status --node 1
# Last log index:   10432
# Commit index:     10432
# Last applied:     10432
# Snapshot index:   9000
# Term:             7
```

### Check secret state

```bash
# Current version of a secret
./bin/meridian-cli secret status --path services/payments/db-password
# Current version: v5
# Created at:      2026-04-06T12:00:00Z
# Rotated at:      2026-04-06T14:00:00Z
# Grace period ends: 2026-04-06T14:05:00Z
# Active leases:   3

# All versions with access counts
./bin/meridian-cli secret versions --path services/payments/db-password
# v4  created 2026-04-05  rotated 2026-04-06  revoked 2026-04-06T14:05:00Z  accesses: 1847
# v5  created 2026-04-06  current                                             accesses: 12
```

### Check lease state

```bash
./bin/meridian-cli lease status --lease-id lease-abc-123
# Identity:   payments-service
# Path:       services/payments/db-password
# Version:    v5
# Issued:     2026-04-06T14:00:00Z
# Expires:    2026-04-06T15:00:00Z
# Renewable:  true
# Status:     active

# All active leases for a secret path
./bin/meridian-cli lease list --path services/payments/db-password
# 3 active leases — payments-service (x2), analytics-service (x1)
```

### Check policy state

```bash
./bin/meridian-cli policy status --path services/payments/db-password
# Matching policies: payments-policy-v2
# Last evaluated:    2026-04-06T14:03:00Z
# Last decision:     ALLOW

./bin/meridian-cli policy eval \
  --identity payments-service \
  --path services/payments/db-password \
  --action read \
  --source-ip 10.4.2.31
# Decision:  ALLOW
# Policy:    payments-policy-v2
# Reason:    identity matches, path matches, source IP in 10.0.0.0/8, within business hours
# Eval time: 0.8ms
```

### Check anomaly state

```bash
./bin/meridian-cli anomaly status --identity payments-service
# Baseline window:     168h (7 days)
# Access count:        4,821
# Typical hours:       06:00–22:00 UTC
# Typical IP range:    10.0.0.0/8
# Typical paths:       services/payments/*
# Recent anomaly score: 0.09 (below threshold 0.7)
# Mode:                alert
```

### Check audit log

```bash
./bin/meridian-cli audit log --path services/payments/db-password --last 20
# INDEX  TIMESTAMP             EVENT                    IDENTITY          DECISION  VERSION
# 10430  2026-04-06T14:03:12Z  secret_access_allowed    payments-service  ALLOW     v5
# 10428  2026-04-06T14:02:55Z  secret_access_allowed    payments-service  ALLOW     v5
# 10415  2026-04-06T14:00:02Z  secret_rotation_complete system            —         v5
# ...

./bin/meridian-cli audit verify --from 0 --to 10432
# Verifying 10432 records...
# Chain valid. No tampering detected.
```

### Prometheus key metrics

```bash
curl http://node1:9090/metrics | grep meridian_

# meridian_requests_by_consistency{level="strong"}         1423
# meridian_requests_by_consistency{level="causal"}         8341
# meridian_requests_by_consistency{level="eventual"}       24891
# meridian_secret_rotations_total                          47
# meridian_lease_expirations_total                         231
# meridian_policy_eval_latency_p99_ms                      1.2
# meridian_anomaly_score{identity="payments-service"}      0.09
# meridian_replication_lag_entries{node="3"}               3
# meridian_quorum_unavailable_total                        0
```

---

## Failure Triage

### No Leader — Cluster Cannot Elect

Symptoms: all nodes report as follower or candidate, no writes accepted, strong reads return `QUORUM_UNAVAILABLE`. All secret access on the strong path is blocked.

1. Check current state of all nodes:
```bash
./bin/meridian-cli cluster status
```

2. Check if terms are incrementing rapidly (election cycling):
```bash
for node in 1 2 3; do
  ./bin/meridian-cli log status --node $node | grep "Term:"
done
```

3. Check network connectivity:
```bash
./bin/meridian-cli ping --from 1 --to 2
./bin/meridian-cli ping --from 1 --to 3
./bin/meridian-cli ping --from 2 --to 3
```

4. Check mTLS certificate validity:
```bash
./bin/meridian-cli cert status --node 1
# If certificates have expired, nodes cannot authenticate and will not vote
```

5. If certificates are expired, rotate them:
```bash
make certs-rotate
# Restart nodes one at a time after certificate rotation
```

6. If the partition is the cause (fewer than quorum nodes can communicate), wait for it to heal. Do not reduce quorum size — this risks split-brain and is more dangerous than temporary unavailability of the secrets platform.

**While the cluster is leaderless:** services holding valid leases can continue reading secrets on the eventual path from any available node, with `STALE_READ: true`. Services that need strong reads must wait. This is the correct behavior.

---

### Secret Rotation Stuck or Incomplete

Symptoms: `meridian-cli secret status` shows a rotation in progress but not completing, or the current pointer and the newest version are inconsistent.

1. Check the rotation state in the audit log:
```bash
./bin/meridian-cli audit log --path <secret-path> --filter rotation --last 10
# Look for: secret_rotation_started, secret_rotation_complete, or secret_rotation_failed
```

2. Check the Raft log for the rotation entry:
```bash
./bin/meridian-cli log search --key "secret:<path>" --last 20
# The rotation should appear as a single log entry containing both the new version
# and the current pointer update
```

3. If the rotation entry is in the log and committed (commit_index ≥ entry index), but `secret status` shows it incomplete — this indicates a bug. Capture diagnostics:
```bash
./bin/meridian-cli secret dump --path <path> > secret_dump_$(date +%s).json
./bin/meridian-cli log dump --node 1 > log_dump_$(date +%s).json
# File a bug with both dumps
```

4. If the rotation entry is not in the log (the leader crashed before reaching quorum), the rotation did not happen. Retry the rotation:
```bash
./bin/meridian-cli secret rotate --path <path>
# Rotation is idempotent if the same version identifier is used
```

5. If services are experiencing access failures because they hold a lease for a version that no longer exists (a partial rotation that left an inconsistent pointer), force-set the current pointer:
```bash
# ⚠️ Destructive — confirm before executing
./bin/meridian-cli secret repair --path <path> --force-version v4
# This writes a new current pointer entry through Raft
# Use the last known good version, not the new version
```

6. After repair, trigger a fresh rotation:
```bash
./bin/meridian-cli secret rotate --path <path>
```

---

### Policy Evaluation Failures

Symptoms: `POLICY_DENIED` on requests that should be allowed, or `POLICY_TIMEOUT` on evaluation.

**POLICY_DENIED — should be allowed:**

1. Check what policy is being evaluated and what decision it produced:
```bash
./bin/meridian-cli policy eval \
  --identity <identity> \
  --path <secret-path> \
  --action read \
  --source-ip <ip>
# Full evaluation trace including which rule matched or did not match
```

2. Check the policy version active on each node:
```bash
for node in 1 2 3; do
  ./bin/meridian-cli policy version --node $node --path <secret-path>
done
# If nodes show different versions, one node is behind on log replication
```

3. If a node is behind, wait for replication to catch up:
```bash
./bin/meridian-cli cluster lag
# Lag should converge to 0 within seconds on a healthy cluster
```

4. If the policy itself is incorrect, roll it back:
```bash
./bin/meridian-cli policy rollback --name <policy-name> --to-version <version>
# Rollback is a strongly consistent write — committed through Raft
```

**POLICY_TIMEOUT — evaluation exceeds fuel budget:**

1. The policy is computationally too expensive. Check evaluation metrics:
```bash
curl http://node1:9090/metrics | grep meridian_policy_eval
# meridian_policy_eval_timeout_total{policy="payments-policy-v2"}  3
```

2. Identify the expensive rule in the policy source and optimize it. Common causes: iteration over unbounded arrays in the input context, nested policy includes, or string operations on long values.

3. Increase the fuel budget temporarily while the policy is rewritten:
```bash
# In environment variables:
MERIDIAN_POLICY_WASM_FUEL_BUDGET=2000000  # default: 1000000
```

4. Upload the optimized policy:
```bash
./bin/meridian-cli policy put --name <policy-name> --file optimized-policy.rego
```

---

### Anomaly Detector False Positives

Symptoms: legitimate service accesses are triggering high anomaly scores; in enforce mode, legitimate accesses are being denied.

1. Check the anomaly score and which features triggered it:
```bash
./bin/meridian-cli anomaly explain \
  --identity <identity> \
  --access-id <access-id-from-audit-log>
# Shows: which features deviated, by how much, and what the baseline was
```

2. Common causes of false positives:
   - A new deployment region introduces a new source IP CIDR
   - A scheduled job runs at an unusual hour
   - A new secret path is accessed by an existing identity

3. If the cause is legitimate (new deployment region), update the baseline exclusions:
```bash
./bin/meridian-cli anomaly baseline update \
  --identity <identity> \
  --allow-ip-cidr 10.5.0.0/16 \
  --reason "new us-east-2 deployment region added 2026-04-06"
```

4. If you are in enforce mode and legitimate access is being blocked, temporarily switch to alert mode:
```bash
./bin/meridian-cli anomaly mode --identity <identity> --mode alert
# This allows access while you investigate; document the reason in the audit note
./bin/meridian-cli audit note \
  --identity <identity> \
  --message "Switched to alert mode for IP baseline update, tracking issue #1234"
```

5. After the baseline has been updated and observed for one window period (default 7 days), switch back to enforce mode.

---

### Split-Brain Recovery

Symptoms: two nodes both report as leader (same or different terms). This must not happen in a correct Raft implementation. If observed, it is a critical bug.

1. **Stop all writes immediately.** Do not attempt to recover without understanding what happened.

2. Capture the state of all nodes:
```bash
for node in 1 2 3; do
  ./bin/meridian-cli log dump --node $node > node${node}_log_$(date +%s).json
  ./bin/meridian-cli secret dump --all --node $node > node${node}_secrets_$(date +%s).json
done
```

3. Determine which node has the higher commit index:
```bash
for node in 1 2 3; do
  ./bin/meridian-cli log status --node $node | grep "Commit index:"
done
```

4. The node with the higher commit index has more committed writes. Shut down all other nodes:
```bash
./bin/meridian-cli node stop --id 2
./bin/meridian-cli node stop --id 3
```

5. Restart the stopped nodes — they will join as followers and receive log replication from the surviving node:
```bash
./bin/meridian-cli node start --id 2
./bin/meridian-cli node start --id 3
```

6. Verify convergence:
```bash
./bin/meridian-cli cluster status
# All nodes: same commit index
```

7. Verify the audit log chain is intact after recovery:
```bash
./bin/meridian-cli audit verify --from 0
# If the chain is broken at the point of divergence, this is evidence of the bug
```

8. File a bug with all log dumps from step 2 and the audit verification result. Split-brain is a correctness violation requiring root cause analysis before the cluster is returned to production.

---

### Log Divergence Repair

Symptoms: a follower's log diverges from the leader's and automatic repair is not converging.

1. Check the follower's log against the leader's:
```bash
./bin/meridian-cli log diff --leader 1 --follower 3
```

2. Check if replication is making progress:
```bash
./bin/meridian-cli replication status --node 3
# Status: converging (replication in progress)
# OR
# Status: stalled (nextIndex not advancing for > 60s)
```

3. If stalled, check for disk space and network issues on the follower:
```bash
df -h /var/lib/meridian/node3
./bin/meridian-cli ping --from 1 --to 3
```

4. If the follower's log is corrupted beyond repair by normal Raft mechanisms:

   **⚠️ Verify the follower has no committed writes not present on the leader before proceeding.**

```bash
./bin/meridian-cli log status --node 1 | grep "Commit index:"
./bin/meridian-cli log status --node 3 | grep "Commit index:"
# Leader commit ≥ follower commit → safe to wipe follower log
```

```bash
./bin/meridian-cli node stop --id 3
rm -rf /var/lib/meridian/node3/raft-log/
# Do NOT remove the WAL — only the raft-log directory
./bin/meridian-cli node start --id 3
# Node 3 requests a snapshot from the leader and applies it
```

5. Verify recovery including secrets and audit state:
```bash
./bin/meridian-cli log diff --leader 1 --follower 3
# Should show: logs identical

./bin/meridian-cli audit verify --from 0
# Should show: chain valid
```

---

### Snapshot Restore Procedure

A node's storage is lost entirely. The node must be restored from a snapshot.

1. Stop the failed node:
```bash
./bin/meridian-cli node stop --id 3
```

2. Clear the node's data directory:
```bash
rm -rf /var/lib/meridian/node3/
mkdir -p /var/lib/meridian/node3/
```

3. Start with `--bootstrap-from-snapshot`:
```bash
./bin/meridian-cli node start --id 3 --bootstrap-from-snapshot
# Node connects to leader, requests latest snapshot,
# receives it via streaming gRPC (chunked), applies it,
# then receives log entries from snapshot index onward
```

4. Monitor the snapshot transfer:
```bash
./bin/meridian-cli node status --id 3
# Status: receiving_snapshot (42% complete, 1.2GB / 2.8GB)
```

5. After application, verify secrets and audit state:
```bash
./bin/meridian-cli cluster status
# Node 3: healthy, same commit index as leader

./bin/meridian-cli audit verify --from 0
# Chain valid — the snapshot includes all audit records up to snapshot index
```

---

### Causal Read Not Satisfied

Symptoms: client receives `CAUSAL_NOT_SATISFIED` repeatedly — the serving node has not seen all events the client's vector clock requires.

1. Check replication lag on the serving node:
```bash
./bin/meridian-cli cluster lag
# High lag causes frequent CAUSAL_NOT_SATISFIED on that node
```

2. If lag is high, identify the cause:
```bash
./bin/meridian-cli replication stats --node 3
# queue_depth: 3200  ← backlog growing faster than being consumed
```

3. For immediate relief, route causal reads away from the lagging node:
```bash
meridian-cli lease renew --lease-id <id> --consistency causal --prefer-nodes 1,2
```

4. For lease renewal specifically: if `CAUSAL_NOT_SATISFIED` persists on all nodes, the client's vector clock may reference events from a snapshot period that the cluster has compacted. Check if the client's VC predates the current snapshot index:
```bash
./bin/meridian-cli log status --node 1 | grep "Snapshot index:"
# If client VC references events before snapshot index, reset the client's VC
# to the snapshot boundary VC returned by any current lease read
```

---

### Audit Chain Verification Failure

Symptoms: `meridian-cli audit verify` reports a chain break at a specific index.

1. Identify the records around the break:
```bash
./bin/meridian-cli audit log --from <index-5> --to <index+5>
# Examine the records on either side of the reported break
```

2. Check all nodes for the same record at the break index:
```bash
for node in 1 2 3; do
  ./bin/meridian-cli audit record --index <break-index> --node $node
done
# If records differ between nodes, this is a replication inconsistency
# If records are the same, this may indicate a bug in the hash chain computation
```

3. Check the audit log around any secret rotation or snapshot boundary at that index:
```bash
./bin/meridian-cli audit log --from <break-index> --filter snapshot-boundary
# A missing snapshot boundary record can break the chain across a compaction
```

4. A chain break is a serious event. Do not dismiss it. Preserve the full audit log dump:
```bash
./bin/meridian-cli audit dump --all > audit_full_$(date +%s).json
```

5. File a critical security incident report with the full dump and the verification output. A chain break may indicate tampering — treat it as such until proven otherwise.

---

## Running the Chaos Suite

The chaos suite is destructive. It runs only in the isolated Docker network.

```bash
# Verify isolated network (fails if not in chaos network)
make chaos-verify

# Run full chaos suite (30 minutes — Phase 8 exit criterion)
make chaos-run

# Run a specific scenario
./chaos/run.py --scenario secret_rotation_under_failover --duration 5m
./chaos/run.py --scenario secret_access_under_partition --duration 10m
./chaos/run.py --scenario policy_enforcement_under_partition --duration 5m
./chaos/run.py --scenario anomaly_injection --duration 5m

# Run the full linearizability check on a captured history
./checker/verify.py --history chaos-run-2026-04-06.json

# Run the audit chain verification on the chaos cluster's log
./bin/meridian-cli audit verify --from 0
```

If the linearizability checker fails, it produces a counterexample: a specific ordering of operations that cannot be explained by a linearizable execution. This is a correctness bug. Do not rerun hoping for a different result. File the counterexample, the cluster configuration, and the full chaos scenario log as a critical bug.

If the audit chain breaks during a chaos run, this is also a critical bug. The hash chain must survive every failure mode — node kills, partitions, clock skew, and snapshot compaction.

---

## Environment Variables Reference

```bash
MERIDIAN_NODE_ID=1
MERIDIAN_PEERS=node2:9090,node3:9090
MERIDIAN_RAFT_PORT=9090
MERIDIAN_CLIENT_PORT=8080
MERIDIAN_DATA_DIR=/var/lib/meridian
MERIDIAN_ELECTION_TIMEOUT_MS=300
MERIDIAN_HEARTBEAT_INTERVAL_MS=50
MERIDIAN_QUORUM_SIZE=2                          # for 3-node cluster
MERIDIAN_WAL_ENCRYPTION_KEY=<32-byte-key>       # AES-256-GCM
MERIDIAN_MEMTABLE_SIZE_MB=64
MERIDIAN_SNAPSHOT_INTERVAL=10000
MERIDIAN_REPLICATION_BUFFER=10000

# Secrets
MERIDIAN_SECRET_ROTATION_GRACE_PERIOD=5m        # old version valid after rotation
MERIDIAN_LEASE_DEFAULT_TTL=1h
MERIDIAN_LEASE_MAX_TTL=24h
MERIDIAN_SECRET_ENCRYPTION_KEK=<32-byte-key>    # key encryption key for DEK wrapping

# Policy
MERIDIAN_POLICY_WASM_MEMORY_LIMIT_MB=64
MERIDIAN_POLICY_WASM_FUEL_BUDGET=1000000
MERIDIAN_POLICY_EVAL_TIMEOUT_MS=10              # hard timeout beyond fuel model

# Anomaly detection
MERIDIAN_ANOMALY_BASELINE_WINDOW_HOURS=168      # 7 days
MERIDIAN_ANOMALY_ALERT_THRESHOLD=0.7
MERIDIAN_ANOMALY_ENFORCE_THRESHOLD=0.85
MERIDIAN_ANOMALY_DEFAULT_MODE=alert             # alert | enforce

# Audit
MERIDIAN_AUDIT_HASH_CHAIN=true
MERIDIAN_AUDIT_HASH_ALGORITHM=sha256

# Observability
MERIDIAN_LOG_LEVEL=info                         # debug | info | warn | error
MERIDIAN_METRICS_PORT=9090
```

---

## See Also

- [Architecture](ARCHITECTURE.md) — system map, component responsibilities, end-to-end flows for each consistency level
- [Design Doc](DESIGN_DOC.md) — Raft log replication, secret rotation protocol, WASM policy sandbox, vector clock algorithm, chaos suite design
- [Tradeoffs](TRADEOFFS.md) — every major design decision with alternatives and reasoning
