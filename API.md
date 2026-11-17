# Meridian API reference

This is the complete client contract for a Meridian storage node. It is
intended for a second engineer who has not read the implementation. The
machine-readable contract is the protobuf service `meridian.kv.KVService` in
[`proto/kv/kv.proto`](proto/kv/kv.proto); generate a client from it with
`protoc`, or use the Go client in [`client/`](client/) or the `meridianctl`
CLI.

## Quick start

```bash
make proto build-storageffi build-tools
make storage-lab OUT=results/trial-1
```

The shortest end-to-end check is the automated fault trial above: it starts
three real Rust-backed nodes with mutual TLS, drives every RPC, crashes the
leader, partitions the cluster, removes quorum and heals it. The expected
output is a JSON summary with `"passed": true` and `"operations": 17`.

For the interactive route, start a secure local cluster and issue one strong
write and read:

```bash
python3 scripts/local-cluster.py          # terminal A; leave running
bin/meridianctl -ca .meridian/local/pki/ca.pem \
  -cert .meridian/local/pki/tenant-a.pem -tls-key .meridian/local/pki/tenant-a.key \
  -op put -key /tenants/a/strong/item -value first -policy-version 1
```

The expected Put result has a positive `raft_index` and
`served_consistency: "CONSISTENCY_STRONG"`. The complete, copy/paste version of
both flows — toolchain setup, regression tests, artifacts, causal context
across nodes, eventual convergence, tenant isolation, troubleshooting and
cleanup — is in [`docs/walkthrough.md`](docs/walkthrough.md).

## Connection and authentication

Every node exposes three listeners:

| Listener | Default local address | Protocol | Who may connect |
|---|---|---|---|
| Client | `MERIDIAN_CLIENT_PORT` (local demo: `127.0.0.1:8081`–`8083`) | gRPC | Registered client identities |
| Peer | `MERIDIAN_RAFT_PORT` (local demo: `127.0.0.1:9091`–`9093`) | gRPC | Registered peer nodes only |
| Metrics | client port + 1000 (local demo: `127.0.0.1:9081`–`9083`) | HTTP | Unauthenticated; keep on trusted networks |

Security is opt-in and all-or-nothing. When `MERIDIAN_TLS_CA`,
`MERIDIAN_TLS_CERT`, `MERIDIAN_TLS_KEY` and `MERIDIAN_CLIENT_POLICIES` are all
set, both gRPC listeners require TLS 1.3 with a client certificate signed by
that CA. A partial configuration refuses to start. When none are set, the node
runs in isolated plaintext development mode.

Identity is the certificate's single URI SAN:

| Workload | URI SAN | Extra requirements |
|---|---|---|
| Node | `urn:meridian:node:<node-id>` | DNS SAN matching the node ID; client and server auth usage |
| Client | `urn:meridian:client:<identity>` | Client auth usage; an entry in the client policy file |

```bash
bin/meridianctl -ca pki/ca.pem -cert pki/tenant-a.pem -tls-key pki/tenant-a.key \
  -op status -targets 127.0.0.1:8081
```

A node certificate cannot use the client listener and a client certificate
cannot use the peer listener. A certificate signed by the right CA but absent
from the policy file is rejected during the handshake. Handshake rejections
surface to the client as `Unavailable`, not `PermissionDenied`, because no RPC
was ever dispatched.

Each client identity is bound to one tenant and to read and/or write
permission. A key belongs to tenant `t` only if it starts with `/tenants/t/`
and is already in canonical form: no `..`, no `//`, no NUL and no backslash.
Authorization runs before any handler, so a denied request never reaches
storage, even for a key that does not exist.

Every request carries a caller-chosen `request_id` (required, non-empty) and an
optional absolute `deadline_unix_nano`. Request IDs are **not** durable
deduplication keys in this prototype.

Errors are standard gRPC statuses. Switch on the code, never on the message
text:

```
rpc error: code = FailedPrecondition desc = namespace policy: policy version 9 does not match current version 1
```

## Endpoint summary

| RPC | Purpose | Permission when secure |
|---|---|---|
| `Status` | Node role, term, leader and commit/apply progress | Any registered client |
| `Get` | Read a key at strong, causal or eventual consistency | Read |
| `Put` | Write a key on its namespace's single write path | Write |
| `Delete` | Delete a key (a tombstone on the weak paths) | Write |
| `CompareAndSet` | Strong conditional write | Read **and** write |

Peer RPCs (`RaftService` in `proto/raft`, and `CausalReplicationService` /
`EventualReplicationService` in `proto/causal`) are internal. They are served
only on the peer listener, and every Raft sender ID must match the calling
node's certificate.

There is no list or scan RPC. `Put` to an existing key replaces it; on the weak
paths a concurrent `Put` from another replica becomes a sibling rather than a
replacement (see [Sibling versions](#sibling-versions)).

## Namespaces and consistency

Every key is an absolute path. An operator assigns each path prefix exactly one
**write class** and a **policy version** at startup with `MERIDIAN_POLICIES`:

```
/tenants/a/strong/:strong:1,/tenants/a/causal/:causal:2,/tenants/a/eventual/:eventual:3
```

The longest matching prefix wins. With no policies configured, the node
installs `/:strong:1` for development. Policies are immutable while the node
runs. Every node in a cluster must be started with the same policies.

| Request | Admitted when |
|---|---|
| Write (`Put`, `Delete`, `CompareAndSet`) | Requested class **equals** the namespace class **and** `context.policy_version` equals the namespace version |
| Read (`Get`) | Requested class is **no stronger** than the namespace class **and** that class is materialized for the namespace |

What each read class can see:

| Namespace class | Strong read | Causal read | Eventual read |
|---|---|---|---|
| strong | yes | yes — strong writes are materialized on the causal path after local apply | yes — likewise materialized |
| causal | `FailedPrecondition` (too strong) | yes | `FailedPrecondition` (not materialized) |
| eventual | `FailedPrecondition` (too strong) | `FailedPrecondition` (too strong) | yes |

No request can upgrade weak data to strong semantics by changing its
`consistency` field. `CONSISTENCY_UNSPECIFIED` is rejected with
`InvalidArgument`.

## Status

### `Status`

Returns the local node's view. It is not proof that the node currently holds a
quorum.

```json
{
  "node_id": "node-1",
  "role": "Follower",
  "term": "1",
  "leader_id": "node-3",
  "commit_index": "0",
  "last_applied": "0",
  "storage_healthy": true
}
```

`role` is `Follower`, `Candidate` or `Leader`. The Go client and
`meridianctl` discover the leader by calling `Status` on each target; they do
not parse error messages.

## Reads

### `Get`

| Field | Required | Meaning |
|---|---:|---|
| `request_id` | Yes | Caller-chosen ID |
| `key` | Yes | Absolute namespace path (bytes) |
| `consistency` | Yes | `CONSISTENCY_STRONG`, `_CAUSAL` or `_EVENTUAL` |
| `context` | No | Causal context returned by an earlier operation |
| `deadline_unix_nano` | No | Absolute deadline; also bounds dependency waits |

**Strong.** The node commits a no-op barrier through Raft, then reads its
local state machine. It must therefore be the leader with a live quorum. A
missing key is a successful response with `found: false`, not an error.

```json
{
  "found": true,
  "value": "Zmlyc3Q=",
  "served_consistency": "CONSISTENCY_STRONG",
  "raft_index": "2",
  "context": null,
  "versions": []
}
```

**Causal.** Any node may serve. If the supplied context names a version or a
Raft index this replica has not applied yet, the read **waits** until it
arrives or the deadline expires. It never returns a value older than the
context.

```json
{
  "found": true,
  "value": "Y2F1c2FsLXZhbHVl",
  "served_consistency": "CONSISTENCY_CAUSAL",
  "raft_index": "6",
  "context": {
    "vector_clock": "AAIABm5vZGUtMQAAAAAAAAABAARyYWZ0AAAAAAAAAAU=",
    "raft_index": "6",
    "policy_version": "2"
  },
  "versions": [{
    "value": "Y2F1c2FsLXZhbHVl",
    "tombstone": false,
    "context": {"vector_clock": "AAIABm5vZGUtMQAAAAAAAAABAARyYWZ0AAAAAAAAAAU=", "raft_index": "0", "policy_version": "2"}
  }]
}
```

**Eventual.** Any node may serve immediately with whatever it has. There is no
dependency wait and `raft_index` is `0`. A read soon after a write on another
node may be missing or stale.

`value` and other bytes fields are base64 in protobuf JSON. `meridianctl` adds
a `value_text` field to Get output for readable demonstrations. Binary callers
should use the protobuf bytes.

#### Sibling versions

On the weak paths, `versions` lists every maximal concurrent version. Meridian
preserves conflicts instead of choosing a timestamp winner.

| `versions` | `found` / `value` |
|---|---|
| empty | `found: false` — key never written on this replica |
| one live version | `found: true`, `value` is that version |
| one tombstone | `found: false` — deleted |
| two or more | `found: false` — **inspect `versions`**; this is not "missing" |

Each version carries its own `context`. A later write on a replica that has
observed every sibling (for causal writes: one whose context covers them all)
supersedes them and resolves the conflict.

## Writes

### `Put` and `Delete`

| Field | Required | Meaning |
|---|---:|---|
| `request_id` | Yes | Caller-chosen ID |
| `key` | Yes | Absolute namespace path |
| `value` | `Put` only | Bytes; empty is a legal value, distinct from absent |
| `consistency` | Yes | Must equal the namespace's write class |
| `context.policy_version` | Yes | Must equal the namespace's policy version |
| `context.vector_clock`, `context.raft_index` | Causal only | Dependencies the write must follow |
| `deadline_unix_nano` | No | Absolute deadline |

**Strong.** The write returns only after a quorum has committed it and the
leader has applied it locally. A local append alone is never acknowledged. The
response carries the commit's `raft_index` and no `context`.

```json
{"raft_index": "1", "served_consistency": "CONSISTENCY_STRONG", "context": null}
```

**Causal.** Accepted locally once the supplied dependencies are visible on this
replica (waiting for them within the deadline). It is then pushed to peers
asynchronously. The returned `context` is what a later operation must carry to
observe this write.

```json
{
  "raft_index": "0",
  "served_consistency": "CONSISTENCY_CAUSAL",
  "context": {"vector_clock": "AAIABm5vZGUtMQAAAAAAAAABAARyYWZ0AAAAAAAAAAU=", "raft_index": "0", "policy_version": "2"}
}
```

**Eventual.** Accepted locally and gossiped immediately. Every node also
re-gossips its full state every 250 ms as anti-entropy, so a missed push is
recovered. That interval is not a convergence deadline.

A causal or eventual `Delete` writes a tombstone version. It replicates and
conflicts like any other write.

### `CompareAndSet`

Strong only. The leader commits a barrier, reads the key, and submits a `Put`
only if the expectation matches. Strong client operations are serialized at the
leader, so the read and the write are atomic with respect to other client
operations.

| Field | Meaning |
|---|---|
| `expected_exists` | `false` means "create only if absent" |
| `expected_value` | Compared byte-for-byte when `expected_exists` is `true` |
| `value` | Replacement on success |
| `consistency` | Must be `CONSISTENCY_STRONG`; causal/eventual return `Unimplemented` |

```json
{"swapped": true,  "current_exists": true, "current_value": "c2Vjb25k", "raft_index": "5", "served_consistency": "CONSISTENCY_STRONG"}
{"swapped": false, "current_exists": true, "current_value": "c2Vjb25k", "raft_index": "6", "served_consistency": "CONSISTENCY_STRONG"}
```

A mismatch is a successful response with `swapped: false`; `current_*` reports
what was there and `raft_index` is the barrier's index. The response's
`context` field is not populated.

## Causal context

The context is an opaque token. Store it and send it back unmodified; never
construct or decode it in a client.

| Field | Meaning |
|---|---|
| `vector_clock` | Stable binary version vector: the causal frontier the client has observed |
| `raft_index` | Strong-path dependency: the replica must have **applied** this Raft index |
| `policy_version` | Namespace policy version the next write is admitted against |

Guarantees:

- A causal read or write carrying a context never observes, or is ordered
  before, state older than that context on **any** node. If the replica is
  behind, the operation waits. When the deadline expires it returns
  `DeadlineExceeded` rather than a stale answer.
- A Raft dependency is satisfied only after the strong write has been applied
  to that node's own storage, not merely received.
- Requests without a context establish no dependency.
- A context's `policy_version` belongs to the namespace that produced it. Do
  not reuse it for a different namespace.

```bash
# Write on node-1, read the same key on node-2 without going back in time.
ctl -op put -consistency causal -policy-version 2 \
  -key /tenants/a/causal/item -value causal-value -context-out ctx.json
ctl -op get -consistency causal -targets 127.0.0.1:8082 \
  -key /tenants/a/causal/item -context-in ctx.json
```

## Errors

| Code | Meaning | Operational response |
|---|---|---|
| `InvalidArgument` | Missing `request_id` or `key`, unspecified consistency, or a malformed context | Fix the caller |
| `FailedPrecondition` | Wrong write class, wrong policy version, read stronger than the namespace, read class not materialized | Fix the class or version; do not retry unchanged |
| `PermissionDenied` | Identity lacks the tenant or the read/write permission | Do not retry with unchanged credentials |
| `Unauthenticated` | Defence in depth: the RPC carried no registered client identity. Unregistered certificates are normally refused earlier, at the handshake (`Unavailable`) | Check the certificate and policy file |
| `Unavailable` | No leader, not the leader, quorum lost, **or** TLS handshake rejected | Check connectivity, leader and credentials; strong ops may be retried on the leader |
| `DeadlineExceeded` | The deadline expired — including waiting for a causal dependency or a quorum | **A mutation's outcome is unknown**; see below |
| `Canceled` | The caller cancelled | As for `DeadlineExceeded` |
| `Unimplemented` | `CompareAndSet` on a weak class | Use a strong namespace |
| `Internal` | Storage or replica failure | Inspect node logs and retain evidence |

A strong write that times out may still commit later. **Do not
automatically retry a timed-out mutation as if it never happened.** Neither
the Go client nor `meridianctl` retries mutations.

#### This contract is measured, not asserted

`make storage-lab` runs the fixed scenario `storage-fault-trial-v1` against
three real node processes and fails loudly if any client-visible outcome
differs from this table. Thirty consecutive runs on November 16, 2026 (Go
1.26.6, Linux amd64, Rust-backed nodes, mutual TLS) all passed, each recording
the same 17 RPCs with the same codes in the same order:

| Phase | RPC | Expected and observed |
|---|---|---|
| Healthy | `strong_write`, `strong_read` | `OK` |
| Tenant isolation | `cross_tenant_{get,put,delete,cas}` | `PermissionDenied` × 4 |
| Read-only identity | `read_only_read` / `read_only_write_denied` | `OK` / `PermissionDenied` |
| Weak replication | `causal_write`, `eventual_write`, then visibility on all 3 nodes | `OK`, converged |
| Missing dependency | `causal_missing_dependency` (context names an unapplied Raft index) | `DeadlineExceeded` |
| Leader process killed | `acknowledged_value_survives_leader_crash` | `OK` from the new leader |
| Leader partitioned | `isolated_strong_write_cannot_acknowledge` | `DeadlineExceeded` (minority never acknowledges) |
| Leader partitioned | `majority_strong_write` / `minority_eventual_write` | `OK` / `OK` (weak path stays available) |
| Partition healed | both values visible on all 3 nodes | converged |
| Quorum lost | `quorum_loss_strong_write_cannot_acknowledge` | `DeadlineExceeded` |
| Quorum restored | `committed_value_after_quorum_recovery` | `OK` |

Every run recorded exactly `PermissionDenied: 5` and `DeadlineExceeded: 3`,
and **every one of those errors was an expected assertion.** No denial reached
storage and no minority acknowledged a strong write.

The latencies are listed for completeness, not as a performance claim:

| Across 30 runs | Min | Median | Max |
|---|---:|---:|---:|
| p50 ms | 39.5 | 63.8 | 83.3 |
| p95 ms | 400.2 | 400.6 | 401.1 |
| Elapsed s | 5.1 | 5.8 | 10.7 |

The p95 is the trial's own 400 ms timeout on the deliberately unacknowledgeable
writes. It is not a tail latency of successful work. The trial is sequential,
includes deliberate failures and excludes discovery polling. It is **not** a
throughput benchmark and not the Q1–Q3 research evaluation.

Repeating the trial is what exposed the last three defects. Before they were
fixed, 3 of 15 runs failed:

| Defect | Symptom | Fix and regression test |
|---|---|---|
| A deposed leader could acknowledge a command that a new leader had overwritten at the same log index (apply waiters were keyed by index only) | A strong write reported success but was discarded | Waiters carry the entry's term; a mismatch returns `Unavailable` (`command was superseded … not committed`). `TestSubmitAndWaitFailsWhenEntryIsSupersededAtSameIndex` |
| Winning an election stopped the election timer permanently | A node that had once been leader never campaigned again; a majority containing it could stay leaderless | Leaders pause the timer; stepping down re-arms it. `TestDeposedLeaderCanCampaignAgain`, `TestElectionTimer_ResetAfterPauseFiresAgain` |
| The trial chose node ports, released them, then created relays that could be assigned the same ports; it also accepted any node reporting `Leader` | A node failed to bind its port; the trial raced a leadership handover | Ports stay reserved until each node starts; a leader must commit a strong-read barrier before the trial uses it. `TestLocalClusterReservesNodePortsUntilStart` |

A failed run is still evidence: keep its directory and investigate it rather
than relabelling it.

## Fault drill

### `bin/meridian-lab`

The lab places a controllable TCP relay on every one-way peer link. It induces
faults by killing real processes and blocking relays.

```bash
bin/meridian-lab -node bin/meridian -out results/trial-2 -timeout 2m
```

**This is an experiment control, not a production API.** Production nodes
expose no fault-injection RPC. A relay block closes established streams and
refuses new ones; it models a symmetric network partition on loopback, not
packet loss or a slow link. Results from the lab are functional evidence for
local processes, and must be reported as such.

| Artifact | Contents |
|---|---|
| `summary.json` | Pass/fail, error, toolchain/OS, RPC count, error counts, p50/p95/p99 |
| `environment.json` | Node binary SHA-256, transport mode, allocated ports |
| `timeline.jsonl` | Ordered RPC outcomes, fault markers and assertions with UTC timestamps |
| `node-*.log` | Real node logs: elections, replication, restarts |
| `before-restart-*`, `partition-*`, `final-*-metrics.txt` | Prometheus snapshots per phase |
| `node-*-data/` | Rust LSM and durable Raft state used by the trial |
| `pki/` | Development CA, certificates, **private keys** and policy file — never publish |

The output directory must not already exist. Child processes are always
cleaned up; data and evidence are retained.

## Operations

### Metrics

`GET http://<node>:<client-port+1000>/metrics` returns Prometheus text. Every
series carries a `node` label. No label contains a key, value, tenant,
credential or request ID.

| Series | Counts |
|---|---|
| `meridian_client_requests_total{method,class,code}` | Completed client RPCs, including authorization denials |
| `meridian_client_request_seconds{method,class}` | Client RPC latency, including admission and dependency waits |
| `meridian_dependency_wait_seconds{code}` | Causal operations that initially had to wait for dependencies |
| `meridian_causal_pending_records` | Received causal records still waiting for dependencies |
| `meridian_replication_attempts_total{class,peer,outcome}` | Causal push / eventual gossip outcomes per peer |
| `meridian_raft_peer_lag_entries{peer}` | Leader's last index minus the peer's confirmed match index; zero on non-leaders |
| `meridian_raft_apply_gap` | Local commit index minus local applied index |
| `meridian_raft_role`, `_current_term`, `_commit_index`, `_elections_total`, `_heartbeats_total` | Raft protocol state (role: 0 follower, 1 candidate, 2 leader) |

These describe different events. Alert on them separately:

- Rising `meridian_causal_pending_records` with flat `meridian_dependency_wait_seconds` means records arrive ahead of their dependencies, but no client is waiting on them yet.
- Rising `meridian_raft_peer_lag_entries` on one peer is that follower falling behind. A rising `meridian_raft_apply_gap` is the local node failing to apply what it has already committed.
- `outcome="ok"` in `meridian_replication_attempts_total` is a peer acknowledgement, not global convergence.

TLS handshake rejections never reach the RPC layer, so they are not counted.
Counters reset on restart; the lab snapshots each phase for that reason.
`GET /healthz` reports that the metrics HTTP server is alive, not that the node
has quorum or healthy storage.

## Clients

**`meridianctl`** wraps every RPC and prints protobuf JSON:

| Flag | Default | Meaning |
|---|---|---|
| `-op` | `status` | `status`, `get`, `put`, `delete`, `cas` |
| `-targets` | `127.0.0.1:8081,…8083` | Strong ops discover the leader across all; weak ops and status use the **first** |
| `-key`, `-value` | | Key and UTF-8 value |
| `-consistency` | `strong` | `strong`, `causal`, `eventual` |
| `-policy-version` | `1` | Namespace policy version on writes |
| `-expected`, `-expected-exists` | `""`, `true` | CAS expectation |
| `-context-in`, `-context-out` | | Read / save a causal context as JSON |
| `-ca`, `-cert`, `-tls-key` | | All three together enable mTLS |
| `-server-name` | | TLS hostname override for tunnels |
| `-timeout` | `5s` | Whole operation, including leader discovery |

**Go** — `client.NewWithCredentials(targets, creds)` uses the same TLS
credentials for leader discovery and operations. `client.New(targets)` is
plaintext, for isolated development only.

## Configuration

| Variable | Required | Meaning |
|---|---:|---|
| `MERIDIAN_NODE_ID` | Yes | This node's ID; must match its certificate |
| `MERIDIAN_RAFT_PORT` | Yes | Peer listener port |
| `MERIDIAN_CLIENT_PORT` | Yes | Client listener port; metrics are served on this + 1000 |
| `MERIDIAN_DATA_DIR` | Yes | Node-private durable directory (Raft state and LSM). Never share between nodes |
| `MERIDIAN_PEERS` | No | `node-2=127.0.0.1:9092,node-3=127.0.0.1:9093`; the legacy form `node-2:9090` uses the host as the ID |
| `MERIDIAN_POLICIES` | No | `prefix:class:version,…`; defaults to `/:strong:1` |
| `MERIDIAN_TLS_CA`, `_TLS_CERT`, `_TLS_KEY` | Secure mode | Trusted CA and this node's certificate/key |
| `MERIDIAN_CLIENT_POLICIES` | Secure mode | JSON array of client rules |

A missing or malformed required variable stops the node at startup with
`failed to load config: …`. The quorum is the majority of peers plus self.
Raft timings are fixed at a 150–300 ms election timeout and a 50 ms heartbeat.

Client policy file:

```json
[
  {"identity": "tenant-a",        "tenant": "a", "read": true, "write": true},
  {"identity": "tenant-a-reader", "tenant": "a", "read": true},
  {"identity": "tenant-b",        "tenant": "b", "read": true, "write": true}
]
```

Unknown fields, duplicate identities, identifiers outside
`[a-z0-9][a-z0-9-]{0,62}` and rules granting nothing are rejected. Use the same
file on every node.

## Configuration boundary

No real secret is needed for local development. `bin/meridian-pki -out <new-dir>`
and the lab generate short-lived development credentials (24-hour leaves; the
CA private key is not kept). Without any `MERIDIAN_TLS_*` variable a node is
intentionally unauthenticated and must only run on an isolated network.
Production-shaped deployments must provision certificates and the client
policy file from a secret manager. Never copy private keys into the
repository, this file or an artifact you publish.

Out of scope in this prototype: certificate rotation and revocation, disk
encryption, tenant quotas, exactly-once mutations and Byzantine fault
tolerance. Tenant isolation is logical access control over one keyspace, not
separate storage. See [`docs/threat-model.md`](docs/threat-model.md) for the
assets, trust boundaries and the regression tests behind each claim.
