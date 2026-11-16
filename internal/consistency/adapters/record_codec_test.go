package adapters

import (
	"testing"

	"github.com/nickemma/meridian/internal/consistency/domain"
)

func TestRecordProtoRoundTrip(t *testing.T) {
	record := domain.Record{
		Key:           []byte("/causal/k"),
		Value:         []byte("v"),
		Version:       domain.VersionVector{"a": 2, "b": 1},
		Dependencies:  domain.VersionVector{"a": 1},
		Origin:        "a",
		RaftIndex:     4,
		PolicyVersion: 3,
	}
	message, err := RecordToProto(record)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := RecordFromProto(message)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Version.Compare(record.Version) != domain.Equal || decoded.Dependencies.Compare(record.Dependencies) != domain.Equal || decoded.Origin != record.Origin || decoded.RaftIndex != record.RaftIndex {
		t.Fatalf("round trip = %#v, want %#v", decoded, record)
	}
}
