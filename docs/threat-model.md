# Storage threat model and security assessment

Meridian remains a distributed key-value research prototype. This document covers its optional authenticated mode, tenant namespaces and the fixed local fault experiment. It does not claim production security or completion of the original Q1–Q3 evaluation.

## Assets and trust boundaries

Assets: stored values and tombstones, Raft terms/votes/logs, causal dependencies, node and client private keys, namespace policies, and client authorization configuration.

Boundaries: client-to-node RPCs, node-to-node replication, the metrics HTTP endpoint, the local filesystem, and the operator who provisions certificates and policies. Authentication and authorization run before storage handlers. The existing consistency-policy admission still decides which write class a namespace supports.

A tenant's keyspace is `/tenants/<tenant>/...`. The certificate identity maps to a tenant in the operator's authorization file; a request cannot select its owner by supplying a header or another tenant's key. CAS requires both read and write permission because its response can reveal the current value.

## Assumptions

- Operators, the local OS/filesystem and authorized replicas are trusted. Raft handles crash faults, not Byzantine members.
- The CA trust root and authorization file are operator-controlled. A CA-signed identity must also be explicitly authorized on the listener it uses.
- Node and client keys are protected. Possession of a valid authorized private key grants that identity's configured access until the credential is effectively removed or expires on a new handshake.
- Replicas hold data for all tenants. Isolation is enforced on client operations; it does not isolate tenant disks, process memory or encryption keys.
- The plaintext development mode is isolated. Supplying any security configuration requires the complete set of TLS files and client policies; there is no fallback after a TLS failure.
- The local metrics endpoint is trusted and reachable only in the development environment. It is unauthenticated, exposes operational metadata and is not a tenant-facing API.

## Security controls and evidence

| Threat | Test and expected outcome | Mitigation and regression evidence |
|---|---|---|
| Unauthenticated/unknown client | Plaintext, foreign-CA and unregistered credentials fail before RPC handlers | Mutual TLS, trust root and explicit identity allowlist; `TestSecureListenerHandshakeAndRoleSeparation` |
| Cross-tenant data access | A client attempts another tenant's Get/Put/Delete/CAS at every requested consistency class; handler is never invoked | Canonical tenant prefix checks; `TestTenantAndPermissionMatrix`; real-process experiment checks all four operations |
| Read-only privilege escalation | A reader attempts Put/Delete/CAS and is denied | Separate read/write rights; CAS requires both; permission matrix and lab reader checks |
| Identity used on wrong listener | Client identities cannot participate as peers; node identities cannot access the client API | Separate peer and client allowlists; handshake role-separation test |
| Peer sender impersonation | A registered peer claims another candidate/leader ID; `PERMISSION_DENIED` | Certificate-bound Raft sender checks; `TestRaftSenderBinding` and real TLS RPC test |
| Namespace confusion | Neighboring tenant prefixes, traversal forms, repeated separators, NUL and backslash keys are denied | A canonical absolute key form, exact `/tenants/<id>/` prefix, no URL decoding; permission matrix |
| Expired local credential | An expired workload certificate is rejected at startup | X.509 validity checks; `TestExpiredLocalCredentialRejected`; new-handshake expiry follows standard TLS validation |
| Misconfigured security | Partial TLS config, wrong local identity, duplicate identities, invalid tenant policy fail startup | Strict startup validation; `TestInvalidConfigurationFailsClosed` |
| Fault-induced strong-data corruption | Leader crash, conflicting suffix after partition and quorum loss must preserve acknowledged strong values | Fixed Rust-backed process trial; `TestHeartbeatDoesNotCommitUnconfirmedSuffix`, `TestSameLengthConflictingSuffixIsReplacedDurably` |

Tests are regression evidence for these scoped properties. They are not an independent penetration test or an exhaustive proof.

## Defects exposed by the new partition experiment

Two added regression tests failed against the original code, then passed with their corresponding fixes:

1. A follower advanced its commit index to the leader's commit index bounded by its own log length. A heartbeat might confirm a shorter common prefix while the follower still held a conflicting suffix. Commit advancement is now bounded by the last index confirmed in that particular AppendEntries request.
2. A same-length conflicting suffix replacement was treated as unchanged because the old/new log lengths matched. The merge now tracks actual changes and persists the replacement; the regression verifies state both before and after reopening the durable Raft store.

The failed development trials are not counted as successful evidence. The automated trial fails on unexpected outcomes and preserves diagnostics.

## Remaining limitations

Certificates are loaded at startup. The development generator creates 24-hour workload certificates, does not persist the CA private key, and refuses to overwrite an existing bundle. Production issuance, automatic rotation, revocation, trust hot reload and connection draining are not implemented. Certificate expiry is checked on new handshakes; established connections are not immediately terminated at expiry. To replace a development bundle, stop the cluster and explicitly generate a new bundle; preserve or discard storage separately.

TLS identities use project-specific `urn:meridian:node:<id>` / `urn:meridian:client:<id>` URI SANs. This is not SPIFFE/SPIRE integration. Standard Go TLS/X.509 implements cryptography. Clients verify server hostname and node role; peers additionally verify the exact configured destination node identity.

No durable client request deduplication is implemented. A mutation that times out may later commit. Clients and the experiment do not silently retry mutations. TLS does not provide application-level exactly-once processing.

Tenant isolation protects key/value access through the client API. Global vector-clock frontiers, Raft indices and node status may reveal aggregate activity; metadata noninterference is not claimed. Values are not encrypted at rest. There are no tenant quotas, handshake flood defenses or connection limits. Trusted replication peers may forward records for any tenant. The existing secret/policy/audit libraries remain separate from this authenticated KV path.

The experiment's TCP relays are local harness components, not a production fault-control API. They cut replication streams; they do not emulate WAN delay/loss/reordering, disk corruption, every crash point or hostile operating systems.
