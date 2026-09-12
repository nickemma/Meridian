# Meridian in Plain Language

## 1. The simple explanation

Meridian is an experimental distributed database for information that does not
all need the same level of protection.

Think of a company with offices in three cities. Some information must be
identical everywhere before anyone is told it changed. Some information only
needs to preserve the order in which one person saw and changed it. Some
information can travel slowly, as long as every office eventually catches up.

Meridian lets the caller choose one of those three paths for each request.

## 2. The three paths

| Everyday example | Meridian path | What it means |
|---|---|---|
| Reserving the last concert ticket | Strong | A majority of database nodes agree before success is returned. This avoids two people both receiving the last ticket. |
| Updating a shopping cart after viewing it | Causal | The system keeps the important “this happened after that” relationship for one user's actions. |
| Recording a page view | Eventual | The local node accepts the event quickly and shares it with the others afterwards. |

The price of stronger protection is waiting for more machines. The benefit of a
weaker path is faster local work, with a narrower promise. Meridian's research
question is whether using all three carefully is worthwhile for a realistic
application.

## 3. How Meridian prevents a dangerous mix

Each family of keys has a fixed label before data is written:

```text
/strong/    ticket and inventory data
/causal/    carts and profiles
/eventual/  views and recommendations
```

A ticket key cannot suddenly accept an eventual write. Meridian rejects that
request before changing data. This rule matters because mixing two different
ways of resolving the same key would make the result hard to explain or trust.

## 4. What happens during trouble

If one city loses contact with the others:

1. A **strong** change may pause because the system refuses to guess without a
   majority.
2. A **causal** change can continue locally when its named earlier changes are
   already present there.
3. An **eventual** change can continue locally and is shared when communication
   returns.

This is intentional. Meridian is not promising that every request always
succeeds. It is promising that each request gets the trade-off it asked for.

## 5. What the prototype has today

The prototype runs three nodes, stores data durably, and exposes a network API
for strong, causal, and eventual requests. It tracks causal dependencies with
version vectors, keeps concurrent eventual values instead of silently choosing
one, and can make a strong update visible to weaker reads after the Raft entry
has applied.

The project has real unit tests, a Rust-backed three-node integration test, and
small local Docker smoke runs. It also has a running three-member etcd
comparison cluster and a Cassandra startup environment. These demonstrate that
the pieces can be run and checked; they are not performance results.

## 6. What it does **not** prove yet

It does not yet prove that Meridian is faster than existing databases, safe
under every failure, or ready for production. Those claims require larger fault
tests, an independent checker, WAN-style measurements, a Meridian YCSB binding,
and repeated comparisons with systems such as etcd and Cassandra. The project
documents those remaining steps instead of substituting estimates for evidence.

## 7. What the comparison setup means

Meridian is being compared with two established systems for different reasons.

| System | Why it is included | What it cannot prove |
|---|---|---|
| etcd | A mature three-member system that keeps each update strongly ordered. | Whether Meridian's causal or eventual paths are better. |
| Cassandra | A system that lets an application choose a replica-consistency level. | That a Cassandra `QUORUM` request is automatically the same as Meridian's strong path. |

The comparison setup is readying the laboratory. Five repeated trials, a fair
dataset, and recorded network conditions are still needed before any chart or
claim is presented.

## 8. A defensive way to describe the project

Use this wording:

> Meridian is a research prototype for a distributed key-value store that
> routes strong, causal, and eventual requests through different replication
> mechanisms while keeping each key on one approved write path. It has working
> three-node integration and deployment smoke coverage. The performance and
> fault-tolerance evaluation is still in progress, so we do not yet claim a
> measured advantage over established systems.

If someone asks why it is useful, explain that many applications already treat
ticket sales, carts, and page views differently. Meridian makes that decision
explicit in one system and measures the cost of doing so.

## 9. A one-minute walkthrough

1. A customer reserves inventory under `/strong/`; Meridian waits for a
   majority before saying yes.
2. The customer adds the item to a cart under `/causal/`; a later cart read
   carries the context needed to see the earlier action.
3. The site records a page view under `/eventual/`; the local node responds
   quickly and other nodes catch up later.
4. If two offices create conflicting eventual values, Meridian returns both
   versions so an application can decide how to resolve them.

That is the project: choose the right promise for each kind of information,
then verify through experiments whether the flexibility is worth its cost.
