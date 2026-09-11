package eventual

import "testing"

type memorySnapshotStore struct{ snapshot []byte }

func (s *memorySnapshotStore) LoadSnapshot() ([]byte, error) {
	return append([]byte(nil), s.snapshot...), nil
}
func (s *memorySnapshotStore) SaveSnapshot(snapshot []byte) error {
	s.snapshot = append([]byte(nil), snapshot...)
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
