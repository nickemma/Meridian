// Package ports names the contracts replicationstore consumes and provides.
package ports

import (
	consistencyports "github.com/nickemma/meridian/internal/consistency/ports"
	storageports "github.com/nickemma/meridian/internal/storage/ports"
)

// KVStore is the durable store snapshots are written to.
type KVStore = storageports.KVStore

// SnapshotStore is the contract this module provides to the replicas.
type SnapshotStore = consistencyports.SnapshotStore
