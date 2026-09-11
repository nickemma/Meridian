package consistency

import (
	"bytes"
	"fmt"
	"sort"
)

// Record is the common version envelope for causal and eventual paths. A
// strong version will be materialized into this form with its committed Raft
// index when cross-path dissemination is implemented.
type Record struct {
	Key           []byte
	Value         []byte
	Tombstone     bool
	Version       VersionVector
	Dependencies  VersionVector
	Origin        string
	RaftIndex     uint64
	PolicyVersion uint64
}

// DependencySatisfied determines whether a replica may expose record to a
// causal read. Version dependencies and strong-path dependencies are separate:
// neither may be inferred from a wall-clock timestamp.
func (r Record) DependencySatisfied(frontier VersionVector, appliedRaftIndex uint64) bool {
	return frontier.Dominates(r.Dependencies) && appliedRaftIndex >= r.RaftIndex
}

// MergeRegister merges an incoming version into a multi-value register. It
// retains exactly the maximal versions: a causally newer update replaces the
// versions it dominates, while concurrent updates remain visible as siblings.
func MergeRegister(existing []Record, incoming Record) ([]Record, error) {
	if err := ValidateRecord(incoming); err != nil {
		return nil, err
	}
	merged := make([]Record, 0, len(existing)+1)
	incomingDominated := false
	for _, record := range existing {
		if err := ValidateRecord(record); err != nil {
			return nil, err
		}
		switch incoming.Version.Compare(record.Version) {
		case After:
			// The incoming record supersedes record.
			continue
		case Before:
			incomingDominated = true
		case Equal:
			if !sameRecord(record, incoming) {
				return nil, fmt.Errorf("equal version vectors carry different records")
			}
			incomingDominated = true
		}
		merged = append(merged, cloneRecord(record))
	}
	if !incomingDominated {
		merged = append(merged, cloneRecord(incoming))
	}
	sort.Slice(merged, func(left, right int) bool {
		leftVersion, _ := merged[left].Version.MarshalBinary()
		rightVersion, _ := merged[right].Version.MarshalBinary()
		if comparison := bytes.Compare(leftVersion, rightVersion); comparison != 0 {
			return comparison < 0
		}
		return merged[left].Origin < merged[right].Origin
	})
	return merged, nil
}

// ValidateRecord rejects malformed envelopes before a transport queues them.
// It is exported so the causal and eventual paths apply exactly the same
// admission rules.
func ValidateRecord(record Record) error {
	if len(record.Key) == 0 {
		return fmt.Errorf("record key is required")
	}
	if record.Origin == "" {
		return fmt.Errorf("record origin is required")
	}
	if len(record.Version) == 0 {
		return fmt.Errorf("record version is required")
	}
	if _, err := record.Version.MarshalBinary(); err != nil {
		return fmt.Errorf("record version: %w", err)
	}
	if _, err := record.Dependencies.MarshalBinary(); err != nil && len(record.Dependencies) != 0 {
		return fmt.Errorf("record dependencies: %w", err)
	}
	return nil
}

func sameRecord(left, right Record) bool {
	return bytes.Equal(left.Key, right.Key) && bytes.Equal(left.Value, right.Value) &&
		left.Tombstone == right.Tombstone && left.Origin == right.Origin &&
		left.RaftIndex == right.RaftIndex && left.PolicyVersion == right.PolicyVersion &&
		left.Dependencies.Compare(right.Dependencies) == Equal
}

func cloneRecord(record Record) Record {
	return Record{
		Key:           bytes.Clone(record.Key),
		Value:         bytes.Clone(record.Value),
		Tombstone:     record.Tombstone,
		Version:       record.Version.Clone(),
		Dependencies:  record.Dependencies.Clone(),
		Origin:        record.Origin,
		RaftIndex:     record.RaftIndex,
		PolicyVersion: record.PolicyVersion,
	}
}
