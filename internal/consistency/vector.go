// Package consistency contains replication metadata shared by Meridian's
// causal and eventual paths. It deliberately has no transport dependency.
package consistency

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

// VersionVector maps replica IDs to their observed counters. Missing entries
// have counter zero. Values are immutable by convention: every mutating method
// returns a fresh vector so callers can safely retain client contexts.
type VersionVector map[string]uint64

type Relation uint8

const (
	Equal Relation = iota
	Before
	After
	Concurrent
)

func (v VersionVector) Clone() VersionVector {
	clone := make(VersionVector, len(v))
	for replica, counter := range v {
		clone[replica] = counter
	}
	return clone
}

func (v VersionVector) Increment(replica string) (VersionVector, error) {
	if replica == "" {
		return nil, fmt.Errorf("replica ID is required")
	}
	clone := v.Clone()
	clone[replica]++
	return clone, nil
}

func (v VersionVector) Merge(other VersionVector) VersionVector {
	merged := v.Clone()
	for replica, counter := range other {
		if counter > merged[replica] {
			merged[replica] = counter
		}
	}
	return merged
}

func (v VersionVector) Compare(other VersionVector) Relation {
	less, greater := false, false
	for replica, counter := range v {
		if counter < other[replica] {
			less = true
		}
		if counter > other[replica] {
			greater = true
		}
	}
	for replica, counter := range other {
		if _, present := v[replica]; !present && counter > 0 {
			less = true
		}
	}
	switch {
	case less && greater:
		return Concurrent
	case less:
		return Before
	case greater:
		return After
	default:
		return Equal
	}
}

func (v VersionVector) Dominates(other VersionVector) bool {
	relation := v.Compare(other)
	return relation == Equal || relation == After
}

// MarshalBinary produces a stable, size-delimited encoding ordered by replica
// ID. A map's randomized iteration order must never enter client contexts or
// replication records.
func (v VersionVector) MarshalBinary() ([]byte, error) {
	keys := make([]string, 0, len(v))
	for replica, counter := range v {
		if replica == "" || counter == 0 {
			return nil, fmt.Errorf("version vector contains invalid replica entry")
		}
		if len(replica) > int(^uint16(0)) {
			return nil, fmt.Errorf("replica ID is too long")
		}
		keys = append(keys, replica)
	}
	sort.Strings(keys)
	if len(keys) > int(^uint16(0)) {
		return nil, fmt.Errorf("version vector has too many replicas")
	}
	var output bytes.Buffer
	if err := binary.Write(&output, binary.BigEndian, uint16(len(keys))); err != nil {
		return nil, err
	}
	for _, replica := range keys {
		if err := binary.Write(&output, binary.BigEndian, uint16(len(replica))); err != nil {
			return nil, err
		}
		if _, err := output.WriteString(replica); err != nil {
			return nil, err
		}
		if err := binary.Write(&output, binary.BigEndian, v[replica]); err != nil {
			return nil, err
		}
	}
	return output.Bytes(), nil
}

func UnmarshalVersionVector(encoded []byte) (VersionVector, error) {
	if len(encoded) < 2 {
		return nil, fmt.Errorf("version vector is truncated")
	}
	count := int(binary.BigEndian.Uint16(encoded[:2]))
	offset := 2
	vector := make(VersionVector, count)
	for index := 0; index < count; index++ {
		if len(encoded)-offset < 2 {
			return nil, fmt.Errorf("version vector replica length is truncated")
		}
		length := int(binary.BigEndian.Uint16(encoded[offset : offset+2]))
		offset += 2
		if length == 0 || len(encoded)-offset < length+8 {
			return nil, fmt.Errorf("version vector replica entry is truncated")
		}
		replica := string(encoded[offset : offset+length])
		offset += length
		counter := binary.BigEndian.Uint64(encoded[offset : offset+8])
		offset += 8
		if counter == 0 {
			return nil, fmt.Errorf("version vector has zero counter")
		}
		if _, duplicate := vector[replica]; duplicate {
			return nil, fmt.Errorf("version vector has duplicate replica %q", replica)
		}
		vector[replica] = counter
	}
	if offset != len(encoded) {
		return nil, fmt.Errorf("version vector has trailing bytes")
	}
	return vector, nil
}
