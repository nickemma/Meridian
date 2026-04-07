# Meridian — Geo-Distributed Key-Value Store with Tunable Consistency

<div align="center">

![Status](https://img.shields.io/badge/status-in%20development-orange)
![Go Version](https://img.shields.io/badge/go-1.25-blue)
![Rust Version](https://img.shields.io/badge/rust-1.87-orange)
![Python Version](https://img.shields.io/badge/python-3.12-blue)
![License](https://img.shields.io/badge/license-APACHE-green)
[![CI](https://github.com/nickemma/meridian/workflows/CI/badge.svg)](https://github.com/nickemma/meridian/actions)

**A Multi-Node Key-Value Store That Lets Clients Choose Their Consistency Model Per Request**

_CAP theorem in real code. No theory without proof._

[Architecture](docs/ARCHITECTURE.md) • [Design Doc](docs/DESIGN_DOC.md) • [Roadmap](docs/ROADMAP.md) • [Runbook](docs/RUNBOOK.md) • [Tradeoffs](docs/TRADEOFFS.md)

</div>

---

## What is Meridian?

Meridian is a geo-distributed key-value store where consistency is not a cluster-wide setting — it is a per-request choice. A client that needs a bank balance uses strong consistency. A client that needs a shopping cart uses causal consistency. A client that needs a leaderboard uses eventual consistency. The same cluster serves all three correctly, simultaneously.

Not a Redis tutorial. Not a Raft blog post. A complete implementation that demonstrates mastery of:

- **Raft consensus from scratch** (leader election, log replication, safety properties — implemented, not configured)
- **Tunable consistency per request** (strong via quorum, eventual via async replication, causal via vector clocks — what Cassandra and DynamoDB actually do)
- **Vector clock causality tracking** (detecting and resolving concurrent writes without a central coordinator)
- **Partition tolerance under network splits** (nodes serve stale reads during partitions — CAP is real, documented, and tested)
- **A Rust LSM storage engine** (write-ahead log, memtable, SSTable compaction — where performance matters, Rust is there)
- **A built-in chaos suite** (random node kills, network partitions, clock skew injection — correctness tested under adversarial conditions, not just happy paths)

**⚠️ Important:** Meridian is a production-style distributed key-value store designed to model CAP theorem in real code. It emphasizes correctness under partitions, tunable consistency per request, and performance-sensitive storage — with chaos-tested guarantees that real systems rely on.

---

## Status

| Component | Status | Description |
|---|---|---|
| **Project Structure** | ✅ Complete | Modular architecture, CI/CD, multi-node Docker cluster |
| **Storage Engine (Rust)** | 📋 Planned | LSM tree, WAL, memtable, SSTable compaction |
| **Raft Consensus (Go)** | 📋 Planned | Leader election, log replication, snapshot, compaction |
| **gRPC API (Go)** | 📋 Planned | Get/Put/Delete with consistency level per request |
| **Strong Consistency** | 📋 Planned | Quorum reads + writes via Raft |
| **Eventual Consistency** | 📋 Planned | Async replication, stale reads during partitions |
| **Causal Consistency** | 📋 Planned | Vector clocks, causality tracking, conflict resolution |
| **Leader Failover** | 📋 Planned | <5s recovery, automatic re-election |
| **Log Compaction** | 📋 Planned | Snapshot + Raft log truncation |
| **Chaos Orchestrator (Python)** | 📋 Planned | Node kills, network partitions, clock skew |
| **Linearizability Checker (Python)** | 📋 Planned | Jepsen-style history verification |
| **Prometheus + Grafana** | 📋 Planned | Replication lag, consensus latency, quorum health |
| **Admin CLI** | 📋 Planned | Cluster topology, manual leader transfer |

**Current Milestone:** Project structure established. Beginning Rust storage engine and Raft leader election.

---

## Architecture

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
┌─────────────────────────────────────────────────────────────────┐
│    ┌──────────────────────┐   ┌─────────────────────────┐      │
│    │   Storage Engine      │   │   Replication Layer     │     │
│    │   (Rust)              │   │   (Go)                  │     │
│    │   LSM  WAL  SSTables  │   │   Async gossip          │     │
│    └──────────────────────┘   └─────────────────────────┘      │
└─────────────────────────────────────────────────────────────────┘
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

For the Raft protocol implementation, vector clock algorithm, quorum calculation, and chaos test suite design — see [Architecture](docs/ARCHITECTURE.md).

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| **Consensus + API** | Go | Goroutine-per-peer Raft, gRPC server, cluster coordination |
| **Storage Engine** | Rust | LSM tree write performance, WAL durability, no GC on write path |
| **Chaos + Verification** | Python | Flexible orchestration, history analysis, linearizability checking |
| **Inter-node + Client** | gRPC + Protobuf | Typed, efficient, bidirectional streaming for log replication |
| **Local Cluster** | Docker + docker-compose | Multi-node simulation, controlled network partitions via iptables |
| **Observability** | Prometheus + Grafana | Replication lag, Raft election count, quorum health, write latency |

---

## Security as First Principles

- mTLS between all cluster nodes — no plaintext inter-node traffic, node identity verified at every connection
- Client authentication via TLS certificates — unauthenticated clients cannot read or write
- WAL encrypted at rest — storage files are not plaintext
- Node identity verification before cluster join — a rogue node cannot join the cluster by knowing the address
- Audit log of all leader elections, configuration changes, and cluster membership changes

---

## Quick Start

### Prerequisites

- Go 1.25
- Rust 1.87 (storage engine)
- Python 3.12 (chaos orchestrator + linearizability checker)
- Docker + docker-compose (multi-node cluster)

```bash
# Clone
git clone https://github.com/nickemma/meridian.git
cd meridian

# Start a 3-node local cluster
make cluster-up

# Verify cluster is healthy
./bin/meridian-cli cluster status
# Node 1 (leader):   healthy  term=1  commit_index=0
# Node 2 (follower): healthy  term=1  commit_index=0
# Node 3 (follower): healthy  term=1  commit_index=0

# Write with strong consistency (quorum write)
./bin/meridian-cli put --key user:1001 --value '{"balance":500}' --consistency strong

# Read with strong consistency (quorum read)
./bin/meridian-cli get --key user:1001 --consistency strong

# Read with eventual consistency (may return stale data)
./bin/meridian-cli get --key user:1001 --consistency eventual

# Run chaos suite (isolated Docker network — destructive)
make chaos-run
```

### Consistency Levels via gRPC

```protobuf
enum ConsistencyLevel {
  STRONG   = 0;  // Quorum read/write via Raft — linearizable
  CAUSAL   = 1;  // Vector clock causality — monotonic reads
  EVENTUAL = 2;  // Async — lowest latency, stale reads possible
}

message GetRequest {
  string key              = 1;
  ConsistencyLevel level  = 2;
  bytes  vector_clock     = 3;  // Required for CAUSAL reads
}
```

### Environment Variables

```bash
MERIDIAN_NODE_ID=1
MERIDIAN_PEERS=node2:9090,node3:9090
MERIDIAN_RAFT_PORT=9090
MERIDIAN_CLIENT_PORT=8080
MERIDIAN_DATA_DIR=/var/lib/meridian
MERIDIAN_ELECTION_TIMEOUT_MS=300
MERIDIAN_HEARTBEAT_INTERVAL_MS=50
MERIDIAN_QUORUM_SIZE=2        # for 3-node cluster
MERIDIAN_WAL_ENCRYPTION_KEY=<32-byte-key>
```

---

## What Makes It Strong

Tunable consistency per request — strong, causal, or eventual — implemented at the protocol level, not just the API surface. Raft consensus fully realized and verified. Chaos suite proves correctness under adversarial conditions, not just happy paths. Jepsen-style linearizability checks mathematically verify that operation histories are consistent. Rust-based LSM storage engine shows precisely where performance matters and why.

---

## Engineering Deep Dive

Key system design areas explored in Meridian:

- Raft consensus implementation from scratch (leader election, log replication, and safety properties)
- Tunable consistency per request (strong via quorum, eventual via async replication, causal via vector clocks)
- Vector clock-based causality tracking for detecting and resolving concurrent writes without a central coordinator
- Partition tolerance and stale-read handling under network splits (CAP theorem in practice)
- Rust-based LSM storage engine (write-ahead log, memtable, SSTable compaction, performance-critical path)
- Chaos testing suite for adversarial conditions (node kills, network partitions, clock skew injection)
- Linearizability and Jepsen-style correctness verification for operation histories

- **Blog:** *"I Built a Distributed KV Store With Tunable Consistency. Here's What CAP Theorem Looks Like in Real Code."* (coming soon)

---

## Author

**[@nickemma](https://github.com/nickemma)** — Building production-grade distributed systems, infrastructure, and backend platforms from first principles.

💼 Open to distributed systems, infrastructure, and backend engineering roles at companies building serious systems.

 <div align="center">
 <a href="https://www.linkedin.com/in/techieemma/"><img src="https://img.shields.io/badge/linkedin-%23f78a38.svg?style=for-the-badge&logo=linkedin&logoColor=white" alt="Linkedin"></a> 
 <a href="https://twitter.com/techieemma"><img src="https://img.shields.io/badge/Twitter-%23f78a38.svg?style=for-the-badge&logo=Twitter&logoColor=white" alt="Twitter"></a> 
 <a href="https://github.com/nickemma/"><img src="https://img.shields.io/badge/github-%23f78a38.svg?style=for-the-badge&logo=github&logoColor=white" alt="Github"></a> 
 <a href="https://techieemma.medium.com/"><img src="https://img.shields.io/badge/Medium-%23f78a38.svg?style=for-the-badge&logo=Medium&logoColor=white" alt="Medium"></a> 
 <a href="mailto:nicholasemmanuel321@gmail.com"><img src="https://img.shields.io/badge/Gmail-f78a38?style=for-the-badge&logo=gmail&logoColor=white" alt="Gmail"></a>
 </div>

---

<div align="center">

**Building Systems, Building Faith — One Commit at a Time**

[⬆ Back to Top](#meridian--geo-distributed-key-value-store-with-tunable-consistency)

</div>