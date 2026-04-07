# Security Policy

Meridian is a distributed storage system. A security vulnerability in a distributed KV store can allow unauthorized reads of sensitive data, unauthorized writes that corrupt stored state, node impersonation that allows a rogue node to join the cluster and participate in consensus, or denial of service that makes the cluster unavailable. In a system that other services build on as a storage primitive, these vulnerabilities propagate to every dependent service.

---

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Email: [nicholasemmanuel321@gmail.com](mailto:nicholasemmanuel321@gmail.com)
Subject: `[SECURITY] Brief description`

Include: what you found, steps to reproduce, and potential impact.

**Response timeline:**
- Acknowledgment within 72 hours
- Critical issues addressed within 48 hours
- You'll be credited in the fix unless you prefer otherwise

---

## Scope

In scope: client authentication bypass (reading or writing without a valid certificate), node identity bypass (joining the cluster without a valid node certificate), mTLS downgrade (inter-node communication falling back to plaintext), WAL decryption without the encryption key, vector clock manipulation that produces causal consistency violations undetectable by the verifier, Raft log corruption that survives the linearizability checker, quorum bypass that allows writes without reaching a majority.

Out of scope: theoretical attacks without proof-of-concept, performance issues, missing features, chaos suite behavior (the chaos suite is intentionally destructive — its behavior is not a vulnerability).

---

## Current Status

Meridian is in active development. Security features are being implemented incrementally — see the [README](../README.md) for current status.

**The chaos suite is destructive and runs in an isolated Docker network. Do not run `make chaos-run` against any real infrastructure. Do not connect a Meridian cluster to systems storing sensitive data until a stable release is tagged.**