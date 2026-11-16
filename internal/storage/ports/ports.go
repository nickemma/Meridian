// Package ports defines Meridian's durable key-value storage boundary.
//
// The production Engine is the Rust LSM reached through its C ABI (see
// adapters, storageffi build tag). Keeping the boundary small prevents the Go
// replication layer from growing a second storage engine.
package ports

import "context"

// Engine is the context-aware durable store owned by one process.
type Engine interface {
	Get(ctx context.Context, key []byte) (value []byte, found bool, err error)
	Put(ctx context.Context, key, value []byte) error
	Delete(ctx context.Context, key []byte) error
	Flush(ctx context.Context) error
	Compact(ctx context.Context) error
	Close() error
}

// KVStore is the narrow synchronous interface used by deterministic state
// machines (Raft apply, replica snapshots). Raft serializes Apply calls, so
// implementations never see direct writes alongside replicated commands.
type KVStore interface {
	Get(key []byte) (value []byte, found bool, err error)
	Put(key, value []byte) error
	Delete(key []byte) error
}
