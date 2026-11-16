// Package adapters is intentionally empty. replicationstore owns no
// infrastructure of its own: the durable KVStore it writes to is supplied by
// storage/adapters (the Rust engine) through storage/application.SyncStore.
package adapters
