# Meridian API and Run Guide

This guide starts the current three-node development deployment and explains the
gRPC API. It is for a trusted local network. The current release has no TLS,
authentication, authorization, request de-duplication, or production-grade
leader-routing client, so do not expose it to the Internet or use it for
security-sensitive data.

## 1. Prerequisites

Install Docker Compose, Go 1.26, Rust with Cargo, and `protoc` with the Go and
gRPC Go plugins. From the repository root, verify the build first:

```bash
make test
make test-strong-integration
```

`test-strong-integration` compiles the Rust storage library, starts three
in-process nodes, and exercises strong, causal, eventual, and cross-path reads.

## 2. Start and stop a local cluster

```bash
make cluster-up
docker compose -f infra/docker-compose.yml ps
docker compose -f infra/docker-compose.yml logs -f node-1 node-2 node-3
```

The local ports are:

| Node | Client gRPC | Peer Raft | Metrics |
|---|---:|---:|---:|
| node-1 | `localhost:8081` | `localhost:9091` | `localhost:9081/metrics` |
| node-2 | `localhost:8082` | `localhost:9092` | `localhost:9082/metrics` |
| node-3 | `localhost:8083` | `localhost:9093` | `localhost:9083/metrics` |

Wait for a `won election` log line. Send strong requests to that node. Causal
and eventual operations may be sent to any running node. Stop the development
cluster with:

```bash
make cluster-down
```

`cluster-down` removes the Compose volumes. Do not use it when you intend to
keep local data.

Prometheus starts with the cluster on `localhost:9099`. Grafana is optional so
an existing service on port 3000 cannot prevent the store from starting:

```bash
docker compose -f infra/docker-compose.yml --profile observability up -d grafana
```

## 3. External baseline services

The pinned etcd and Cassandra references are isolated from Meridian:

```bash
make baselines-up
docker compose -f infra/baselines-compose.yml ps
make baselines-down
```

etcd v3.5.18 exposes a three-member client endpoint set on ports `23791`–
`23793`; its quorum health check has passed locally. Cassandra 4.1.8 exposes
CQL on `localhost:9042`. Its Compose service is a startup and schema smoke
environment only. It does not yet provide the three replicas required for a
meaningful `ONE`/`QUORUM` comparison.

YCSB source is pinned locally at version 0.17.0, commit
`4b19340e3bab5e4c88eda75ad56e83dc4d5cc503`. A Meridian YCSB binding and
retained baseline trials remain unfinished. Cassandra is a per-consistency-level
reference, not a linearizability baseline; record its selected consistency level
with every trial.

## 4. Namespace policies

Each key belongs to one immutable, longest-prefix policy. The Compose setup
installs these policies:

| Prefix | Write class | Policy version |
|---|---|---:|
| `/strong/` | strong | 1 |
| `/causal/` | causal | 2 |
| `/eventual/` | eventual | 3 |

For a custom deployment, set the same value on every node before startup:

```text
MERIDIAN_POLICIES=/strong/:strong:1,/causal/:causal:2,/eventual/:eventual:3
```

The format is `prefix:class:version`, separated by commas. Prefixes must be
`/` or end in `/`. A write must use the exact class and policy version assigned
to its key. A mismatch returns gRPC `FAILED_PRECONDITION` before mutation.

## 5. Smoke-test the API

Use the included closed-loop driver after identifying the leader. For example,
if node-2 is leader:

```bash
go run ./cmd/meridian-load \
  -target localhost:8082 -consistency strong -policy-version 1 \
  -operations 16 -concurrency 1 -read-percent 50 \
  -raw /tmp/strong-history.jsonl

go run ./cmd/meridian-check -history /tmp/strong-history.jsonl -limit 20

go run ./cmd/meridian-load \
  -target localhost:8082 -consistency causal -policy-version 2 \
  -operations 16 -concurrency 1 -read-percent 50

go run ./cmd/meridian-load \
  -target localhost:8082 -consistency eventual -policy-version 3 \
  -operations 16 -concurrency 1 -read-percent 50
```

`meridian-check` is exhaustive for its bounded single-register input. It is a
development smoke checker, not a substitute for a large-history checker. Start
from an empty data directory or include initialization writes in the retained
history; otherwise a conditional update can correctly observe state that the
checker has no record of.

## 6. gRPC contract

The canonical schema is [`proto/kv/kv.proto`](proto/kv/kv.proto). The service
is `meridian.kv.KVService`:

| RPC | Purpose |
|---|---|
| `Get` | Fetch a value or all maximal causal/eventual versions. |
| `Put` | Write a value through the namespace's assigned path. |
| `Delete` | Write a tombstone through the assigned path. |
| `CompareAndSet` | Strong conditional update. |
| `Status` | Return node ID, Raft role, leader ID, term, and applied index. |

Every data request needs a non-empty `request_id`, key, and consistency enum.
`deadline_unix_nano` is optional; use a normal gRPC context deadline as well.

`CausalContext` carries `vector_clock`, `raft_index`, and `policy_version`.
Clients must keep the context returned by a successful causal operation and
supply it to dependent causal reads and writes. All writes, including strong
writes, must set `policy_version` to the policy revision for their key.

For a causal or eventual `Get`, inspect `versions`. A single non-tombstoned
version is also reflected in `found` and `value`. Multiple entries mean
concurrent values exist; a caller must resolve them with a later write rather
than choosing one silently.

## 7. Go client example

Generated types are in `github.com/nickemma/meridian/proto/kv`.

```go
connection, err := grpc.NewClient("localhost:8082",
    grpc.WithTransportCredentials(insecure.NewCredentials()))
if err != nil { log.Fatal(err) }
defer connection.Close()

client := kv.NewKVServiceClient(connection)
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

put, err := client.Put(ctx, &kv.PutRequest{
    RequestId:  "cart-write-1",
    Key:        []byte("/causal/cart/alice"),
    Value:      []byte("item-42"),
    Consistency: kv.Consistency_CONSISTENCY_CAUSAL,
    Context:    &kv.CausalContext{PolicyVersion: 2},
})
if err != nil { log.Fatal(err) }

read, err := client.Get(ctx, &kv.GetRequest{
    RequestId:  "cart-read-1",
    Key:        []byte("/causal/cart/alice"),
    Consistency: kv.Consistency_CONSISTENCY_CAUSAL,
    Context:    put.Context,
})
if err != nil { log.Fatal(err) }
fmt.Printf("found=%t value=%q versions=%d\n", read.Found, read.Value, len(read.Versions))
```

For a strong write, use `github.com/nickemma/meridian/client`. Construct it
with all client addresses; it discovers the leader before a strong request.
It does not automatically retry a mutation after `Unavailable`, because
durable request de-duplication is not implemented. The application must decide
whether retrying an ambiguous write is safe.

```go
cluster, err := client.New([]string{"localhost:8081", "localhost:8082", "localhost:8083"})
if err != nil { log.Fatal(err) }
defer cluster.Close()
response, err := cluster.Put(ctx, strongPutRequest)
```

## 8. Error handling

| Code | Meaning | Client action |
|---|---|---|
| `InvalidArgument` | Missing request ID/key, invalid context, or unspecified class | Fix the request. |
| `FailedPrecondition` | Policy mismatch, stale policy version, or local causal dependency unavailable | Refresh policy/context or wait for the dependency. |
| `Unavailable` | Target is not leader or a leader is unavailable | Discover the leader; use an application-safe retry policy. |
| `DeadlineExceeded` | Client or request deadline elapsed | Record the ambiguous outcome before deciding whether to retry. |
| `Internal` | Local storage or replication-path failure | Treat as failed; preserve the operation record for diagnosis. |

## 9. Open-loop research driver

Build the driver, then run the fixed mixed workload against all three nodes:

```bash
make build-openload
mkdir -p results/mixed-smoke
./bin/meridian-openload \
  -targets localhost:8081,localhost:8082,localhost:8083 \
  -workload bench/workloads/mixed.json \
  -duration 30s \
  -rate 100 \
  -max-inflight 128 \
  -raw results/mixed-smoke/history.jsonl \
  2>results/mixed-smoke/summary.json
```

The JSONL file retains scheduled, invoked, and completed times for every
request, returned versions, response context, Raft index, and conditional-write
outcomes. Analyze a completed history with:

```bash
make build-staleness
./bin/meridian-staleness -history results/mixed-smoke/history.jsonl \
  >results/mixed-smoke/staleness.json
```

The analyzer reports only staleness that the client history can establish. It
excludes concurrent weak writes and reports a lower-bound elapsed time from a
missed write's completion to the response; it does not claim a propagation
delay or a universal total order. The driver and analyzer are harness tools;
follow the protocol in
[`docs/research.md`](docs/research.md) before treating its output as a result.

## 10. Current limits

Meridian is a research prototype. It has functional three-node and Docker smoke
coverage, a healthy local three-member etcd reference deployment, and a
Cassandra startup environment. It does not yet have retained WAN trials,
completed external-baseline measurements, an acknowledgement-to-visibility
staleness measure, a scalable history checker, TLS, authentication, or a
production recovery/backup procedure. See [`docs/research.md`](docs/research.md)
and [`docs/ROADMAP.md`](docs/ROADMAP.md) before making performance or safety
claims.
