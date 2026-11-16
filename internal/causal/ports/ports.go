// Package ports names what the causal replica requires from infrastructure.
package ports

import consistencyports "github.com/nickemma/meridian/internal/consistency/ports"

// SnapshotStore persists the replica's complete state after every change.
type SnapshotStore = consistencyports.SnapshotStore
