// Package ports declares the contracts the consistency kernel offers to, and
// requires from, the rest of Meridian.
package ports

import "github.com/nickemma/meridian/internal/consistency/domain"

// PolicyResolver selects and admits namespace policies. Every client path
// (strong, causal and eventual) consults the same resolver.
type PolicyResolver interface {
	Lookup(key string) (domain.NamespacePolicy, error)
	ValidateWrite(key string, requested domain.Class, expectedVersion uint64) (domain.NamespacePolicy, error)
	ValidateRead(key string, requested domain.Class) (domain.NamespacePolicy, error)
}

// SnapshotStore stores one complete, self-validating replica snapshot. A
// missing snapshot is represented by nil bytes and no error. It is shared by
// the causal and eventual replicas so both persist through one contract.
type SnapshotStore interface {
	LoadSnapshot() ([]byte, error)
	SaveSnapshot([]byte) error
}

// ReplicationObserver receives the outcome of one outbound weak-path RPC.
type ReplicationObserver interface {
	Replicated(class, peer string, err error)
}
