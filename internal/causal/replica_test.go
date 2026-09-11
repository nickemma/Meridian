package causal

import (
	"errors"
	"testing"

	"github.com/nickemma/meridian/internal/consistency"
)

type memorySnapshotStore struct{ snapshot []byte }

func (s *memorySnapshotStore) LoadSnapshot() ([]byte, error) {
	return append([]byte(nil), s.snapshot...), nil
}
func (s *memorySnapshotStore) SaveSnapshot(snapshot []byte) error {
	s.snapshot = append([]byte(nil), snapshot...)
	return nil
}

func TestReplicaHoldsReorderedRecordUntilDependencyArrives(t *testing.T) {
	origin, err := NewReplica("origin")
	if err != nil {
		t.Fatal(err)
	}
	first, err := origin.Write([]byte("/causal/a"), []byte("one"), false, nil, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := origin.Write([]byte("/causal/b"), []byte("two"), false, first.Version, 0, 1)
	if err != nil {
		t.Fatal(err)
	}

	target, err := NewReplica("target")
	if err != nil {
		t.Fatal(err)
	}
	applied, err := target.Receive(second)
	if err != nil || applied {
		t.Fatalf("Receive(second) = (%v, %v), want (false, nil)", applied, err)
	}
	if target.Pending() != 1 {
		t.Fatalf("pending = %d, want 1", target.Pending())
	}
	applied, err = target.Receive(first)
	if err != nil || !applied {
		t.Fatalf("Receive(first) = (%v, %v), want (true, nil)", applied, err)
	}
	got, err := target.Read([]byte("/causal/b"), second.Version, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Values) != 1 || string(got.Values[0].Value) != "two" || target.Pending() != 0 {
		t.Fatalf("applied record = %#v, pending = %d", got.Values, target.Pending())
	}
}

func TestReplicaEnforcesClientAndRaftDependencies(t *testing.T) {
	replica, err := NewReplica("a")
	if err != nil {
		t.Fatal(err)
	}
	missing := consistency.VersionVector{"other": 1}
	if _, err := replica.Write([]byte("/causal/k"), []byte("v"), false, missing, 0, 1); !errors.Is(err, ErrUnsatisfiedDependencies) {
		t.Fatalf("Write missing vector dependency error = %v", err)
	}
	if _, err := replica.Write([]byte("/causal/k"), []byte("v"), false, nil, 4, 1); !errors.Is(err, ErrUnsatisfiedDependencies) {
		t.Fatalf("Write missing Raft dependency error = %v", err)
	}
	replica.ObserveRaft(4)
	record, err := replica.Write([]byte("/causal/k"), []byte("v"), false, nil, 4, 1)
	if err != nil {
		t.Fatal(err)
	}
	if record.RaftIndex != 4 {
		t.Fatalf("RaftIndex = %d, want 4", record.RaftIndex)
	}
}

func TestReplicaKeepsConcurrentValuesAndIsIdempotent(t *testing.T) {
	left, err := NewReplica("left")
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewReplica("right")
	if err != nil {
		t.Fatal(err)
	}
	leftWrite, err := left.Write([]byte("/causal/k"), []byte("left"), false, nil, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	rightWrite, err := right.Write([]byte("/causal/k"), []byte("right"), false, nil, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewReplica("target")
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []consistency.Record{leftWrite, rightWrite, leftWrite} {
		if _, err := target.Receive(record); err != nil {
			t.Fatal(err)
		}
	}
	got, err := target.Read([]byte("/causal/k"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Values) != 2 {
		t.Fatalf("values = %#v, want two concurrent siblings", got.Values)
	}
}

func TestReplicaDoesNotTreatCausalReceiptAsStrongApplication(t *testing.T) {
	replica, err := NewReplica("a")
	if err != nil {
		t.Fatal(err)
	}
	record := consistency.Record{
		Key:          []byte("/causal/k"),
		Value:        []byte("v"),
		Version:      consistency.VersionVector{"origin": 1},
		Dependencies: consistency.VersionVector{},
		Origin:       "origin",
		RaftIndex:    7,
	}
	applied, err := replica.Receive(record)
	if err != nil || applied {
		t.Fatalf("Receive = (%v, %v), want held by Raft watermark", applied, err)
	}
	replica.ObserveRaft(6)
	if replica.Pending() != 1 {
		t.Fatalf("record released before local Raft apply")
	}
	replica.ObserveRaft(7)
	if replica.Pending() != 0 {
		t.Fatalf("record not released after local Raft apply")
	}
}

func TestOpenReplicaRestoresRecordsAndRaftWatermark(t *testing.T) {
	store := &memorySnapshotStore{}
	first, err := OpenReplica("a", store)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.ObserveRaft(5); err != nil {
		t.Fatal(err)
	}
	record, err := first.Write([]byte("/causal/k"), []byte("v"), false, nil, 5, 1)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenReplica("a", store)
	if err != nil {
		t.Fatal(err)
	}
	got, err := restarted.Read([]byte("/causal/k"), record.Version, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Values) != 1 || string(got.Values[0].Value) != "v" || got.AppliedRaftIdx != 5 {
		t.Fatalf("restored snapshot = %#v", got)
	}
}
