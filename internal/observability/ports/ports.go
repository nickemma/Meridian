// Package ports declares the observation hooks the storage paths call.
package ports

import (
	"time"

	consistencyports "github.com/nickemma/meridian/internal/consistency/ports"
)

// ReplicationObserver records one outbound weak-path RPC outcome.
type ReplicationObserver = consistencyports.ReplicationObserver

// DependencyObserver records how long a causal operation waited for its
// dependencies, labelled by the final RPC outcome.
type DependencyObserver interface {
	Waited(start time.Time, err error)
}
