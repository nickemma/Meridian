// Package ports names what the eventual replica requires from infrastructure.
package ports

import (
	consistency "github.com/nickemma/meridian/internal/consistency/domain"
	consistencyports "github.com/nickemma/meridian/internal/consistency/ports"
)

// SnapshotStore persists the replica's complete state after every change.
type SnapshotStore = consistencyports.SnapshotStore

// Gossip sends a batch of records to every peer asynchronously.
type Gossip func([]consistency.Record)
