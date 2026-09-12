package eventual

import (
	"bytes"
	"sort"
	"strings"
	"testing"

	"github.com/nickemma/meridian/internal/consistency"
)

type memorySnapshotStore struct {
	snapshot []byte
	saves    int
}

func (s *memorySnapshotStore) LoadSnapshot() ([]byte, error) {
	return append([]byte(nil), s.snapshot...), nil
}
func (s *memorySnapshotStore) SaveSnapshot(snapshot []byte) error {
	s.snapshot = append([]byte(nil), snapshot...)
	s.saves++
	return nil
}

func TestReplicasConvergeAfterConcurrentWrites(t *testing.T) {
	left, err := NewReplica("left")
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewReplica("right")
	if err != nil {
		t.Fatal(err)
	}
	leftWrite, err := left.Write([]byte("/eventual/k"), []byte("left"), false, 1)
	if err != nil {
		t.Fatal(err)
	}
	rightWrite, err := right.Write([]byte("/eventual/k"), []byte("right"), false, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := left.Receive(rightWrite); err != nil {
		t.Fatal(err)
	}
	if err := right.Receive(leftWrite); err != nil {
		t.Fatal(err)
	}
	leftValues := left.Read([]byte("/eventual/k")).Values
	rightValues := right.Read([]byte("/eventual/k")).Values
	if len(leftValues) != 2 || len(rightValues) != 2 {
		t.Fatalf("left=%#v right=%#v, want two concurrent values", leftValues, rightValues)
	}
}

func TestWriteAfterReceiveSupersedesObservedVersion(t *testing.T) {
	origin, err := NewReplica("origin")
	if err != nil {
		t.Fatal(err)
	}
	first, err := origin.Write([]byte("/eventual/k"), []byte("one"), false, 1)
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewReplica("target")
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Receive(first); err != nil {
		t.Fatal(err)
	}
	second, err := target.Write([]byte("/eventual/k"), []byte("two"), false, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := origin.Receive(second); err != nil {
		t.Fatal(err)
	}
	values := origin.Read([]byte("/eventual/k")).Values
	if len(values) != 1 || string(values[0].Value) != "two" {
		t.Fatalf("values=%#v, want only newer value", values)
	}
}

func TestOpenReplicaRestoresRegister(t *testing.T) {
	store := &memorySnapshotStore{}
	first, err := OpenReplica("a", store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Write([]byte("/eventual/k"), []byte("v"), false, 1); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenReplica("a", store)
	if err != nil {
		t.Fatal(err)
	}
	got := restarted.Read([]byte("/eventual/k"))
	if len(got.Values) != 1 || string(got.Values[0].Value) != "v" {
		t.Fatalf("restored values = %#v", got.Values)
	}
}

func TestPartitionHealConvergesAndPreservesConcurrentSiblings(t *testing.T) {
	left, err := NewReplica("left")
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewReplica("right")
	if err != nil {
		t.Fatal(err)
	}
	witness, err := NewReplica("witness")
	if err != nil {
		t.Fatal(err)
	}

	// left and right are partitioned and can both acknowledge local writes.
	leftWrite, err := left.Write([]byte("/eventual/k"), []byte("left"), false, 1)
	if err != nil {
		t.Fatal(err)
	}
	rightWrite, err := right.Write([]byte("/eventual/k"), []byte("right"), false, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Healing may deliver updates in different orders and repeat them. Every
	// replica must converge to the same two maximal siblings.
	for _, replica := range []*Replica{left, right, witness} {
		for _, record := range []consistency.Record{rightWrite, leftWrite, rightWrite, leftWrite} {
			if err := replica.Receive(record); err != nil {
				t.Fatal(err)
			}
		}
	}
	leftValues := left.Read([]byte("/eventual/k")).Values
	if len(leftValues) != 2 {
		t.Fatalf("left values = %#v, want two concurrent siblings", leftValues)
	}
	want := recordSet(leftValues)
	for _, replica := range []*Replica{right, witness} {
		if got := recordSet(replica.Read([]byte("/eventual/k")).Values); !bytes.Equal(got, want) {
			t.Fatalf("replica did not converge: got %q, want %q", got, want)
		}
	}
}

func TestReceiveAllMergesBatchWithOneCheckpoint(t *testing.T) {
	store := &memorySnapshotStore{}
	replica, err := OpenReplica("target", store)
	if err != nil {
		t.Fatal(err)
	}
	first := consistency.Record{Key: []byte("/eventual/k"), Value: []byte("one"), Version: consistency.VersionVector{"left": 1}, Origin: "left", PolicyVersion: 1}
	second := consistency.Record{Key: []byte("/eventual/k"), Value: []byte("two"), Version: consistency.VersionVector{"right": 1}, Origin: "right", PolicyVersion: 1}
	if err := replica.ReceiveAll([]consistency.Record{first, second, first}); err != nil {
		t.Fatal(err)
	}
	if store.saves != 1 {
		t.Fatalf("snapshot saves = %d, want 1", store.saves)
	}
	if values := replica.Read([]byte("/eventual/k")).Values; len(values) != 2 {
		t.Fatalf("values = %#v, want two concurrent siblings", values)
	}
}

func recordSet(records []consistency.Record) []byte {
	values := make([]string, 0, len(records))
	for _, record := range records {
		encoded, _ := record.Version.MarshalBinary()
		values = append(values, string(encoded)+":"+string(record.Value))
	}
	sort.Strings(values)
	return []byte(strings.Join(values, "\x00"))
}
