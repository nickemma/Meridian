package consistency

import "testing"

func testRecord(value string, version VersionVector) Record {
	return Record{Key: []byte("key"), Value: []byte(value), Version: version, Origin: "node-a", PolicyVersion: 1}
}

func TestMergeRegisterRemovesDominatedVersion(t *testing.T) {
	old := testRecord("old", VersionVector{"a": 1})
	newer := testRecord("new", VersionVector{"a": 2})
	merged, err := MergeRegister([]Record{old}, newer)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(merged) != 1 || string(merged[0].Value) != "new" {
		t.Fatalf("merged records = %+v, want only new", merged)
	}
}

func TestMergeRegisterRetainsConcurrentSiblings(t *testing.T) {
	left := testRecord("left", VersionVector{"a": 1})
	right := testRecord("right", VersionVector{"b": 1})
	merged, err := MergeRegister([]Record{left}, right)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(merged) != 2 {
		t.Fatalf("sibling count = %d, want 2", len(merged))
	}
}

func TestRecordDependencySatisfied(t *testing.T) {
	record := Record{Dependencies: VersionVector{"a": 2}, RaftIndex: 4}
	if record.DependencySatisfied(VersionVector{"a": 2}, 3) {
		t.Fatal("record was visible before Raft dependency applied")
	}
	if record.DependencySatisfied(VersionVector{"a": 1}, 4) {
		t.Fatal("record was visible before vector dependency applied")
	}
	if !record.DependencySatisfied(VersionVector{"a": 2, "b": 1}, 4) {
		t.Fatal("record was not visible after all dependencies applied")
	}
}

func TestMergeRegisterRejectsEqualVersionWithDifferentValue(t *testing.T) {
	first := testRecord("first", VersionVector{"a": 1})
	second := testRecord("second", VersionVector{"a": 1})
	if _, err := MergeRegister([]Record{first}, second); err == nil {
		t.Fatal("merge accepted incompatible equal versions")
	}
}
