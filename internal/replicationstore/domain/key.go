// Package domain defines where replication-path metadata lives in the shared
// LSM keyspace. The binary-prefixed keys are outside the client namespace
// (which requires absolute paths) and are never exposed through the KV API.
package domain

import "fmt"

const reservedPrefix = "\x00meridian/replication/"

// SnapshotKey returns the reserved storage key for one mechanism's snapshot.
func SnapshotKey(mechanism string) ([]byte, error) {
	if mechanism == "" {
		return nil, fmt.Errorf("mechanism is required")
	}
	return []byte(reservedPrefix + mechanism + "/snapshot"), nil
}
