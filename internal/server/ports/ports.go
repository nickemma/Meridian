// Package ports declares what the KV use cases need from consensus, storage
// and the weak-path replicas. The composition root supplies the adapters.
package ports

import (
	"context"
	"time"

	causal "github.com/nickemma/meridian/internal/causal/domain"
	consistency "github.com/nickemma/meridian/internal/consistency/domain"
	eventual "github.com/nickemma/meridian/internal/eventual/domain"
	raft "github.com/nickemma/meridian/internal/raft/domain"
)

// Consensus is the strong path: a Raft node that commits and applies.
type Consensus interface {
	SubmitAndWait(ctx context.Context, command []byte) (uint64, error)
	LinearizableBarrier(ctx context.Context) (uint64, error)
	Status() raft.Status
}

// StateReader reads the local state machine after a linearizable barrier.
type StateReader interface {
	Get(key []byte) (value []byte, found bool, err error)
}

// CausalReplica is the causal path's local replica.
type CausalReplica interface {
	ReadContext(ctx context.Context, key []byte, required consistency.VersionVector, raftIndex uint64) (causal.Snapshot, error)
	WriteContext(ctx context.Context, key, value []byte, tombstone bool, dependencies consistency.VersionVector, raftIndex, policyVersion uint64) (consistency.Record, error)
	DependenciesSatisfied(required consistency.VersionVector, raftIndex uint64) bool
}

// EventualReplica is the eventual path's local replica.
type EventualReplica interface {
	Read(key []byte) eventual.Snapshot
	Write(key, value []byte, tombstone bool, policyVersion uint64) (consistency.Record, error)
}

// Publish disseminates one causal record; Gossip sends an eventual batch.
type (
	Publish func(consistency.Record)
	Gossip  func([]consistency.Record)
)

// DependencyObserver records causal dependency waits. err is the request's
// final classified error (or nil).
type DependencyObserver interface {
	Waited(start time.Time, err error)
}

// CausalMaterializer receives strong writes and the applied-Raft watermark.
type CausalMaterializer interface {
	ObserveRaft(index uint64) error
	Receive(record consistency.Record) (bool, error)
}

// EventualMaterializer receives strong writes for eventual reads.
type EventualMaterializer interface {
	Receive(record consistency.Record) error
}
