package consistency

import (
	"bytes"
	"testing"
)

func TestVersionVectorComparison(t *testing.T) {
	cases := []struct {
		name     string
		left     VersionVector
		right    VersionVector
		relation Relation
	}{
		{"equal", VersionVector{"a": 1}, VersionVector{"a": 1}, Equal},
		{"before", VersionVector{"a": 1}, VersionVector{"a": 2}, Before},
		{"after", VersionVector{"a": 2, "b": 1}, VersionVector{"a": 2}, After},
		{"concurrent", VersionVector{"a": 2, "b": 1}, VersionVector{"a": 1, "b": 2}, Concurrent},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := test.left.Compare(test.right); got != test.relation {
				t.Fatalf("Compare = %d, want %d", got, test.relation)
			}
		})
	}
}

func TestVersionVectorMergeAndIncrementDoNotMutateInputs(t *testing.T) {
	base := VersionVector{"a": 2}
	incremented, err := base.Increment("b")
	if err != nil {
		t.Fatalf("increment: %v", err)
	}
	merged := incremented.Merge(VersionVector{"a": 3, "c": 1})
	if base["b"] != 0 || incremented["a"] != 2 {
		t.Fatal("vector operation mutated its input")
	}
	if !merged.Dominates(VersionVector{"a": 3, "b": 1, "c": 1}) {
		t.Fatalf("merged vector %v does not dominate expected vector", merged)
	}
}

func TestVersionVectorBinaryEncodingIsStableAndRoundTrips(t *testing.T) {
	vector := VersionVector{"z": 4, "a": 1}
	first, err := vector.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal first: %v", err)
	}
	second, err := VersionVector{"a": 1, "z": 4}.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal second: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("binary encodings differ: %x vs %x", first, second)
	}
	decoded, err := UnmarshalVersionVector(first)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Compare(vector) != Equal {
		t.Fatalf("decoded vector = %v, want %v", decoded, vector)
	}
}

func TestVersionVectorRejectsInvalidEncodings(t *testing.T) {
	for _, encoded := range [][]byte{nil, {0}, {0, 1, 0, 0}, {0, 1, 0, 1, 'a'}} {
		if _, err := UnmarshalVersionVector(encoded); err == nil {
			t.Fatalf("UnmarshalVersionVector(%x) succeeded", encoded)
		}
	}
}
