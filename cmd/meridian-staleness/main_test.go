package main

import (
	"encoding/base64"
	"testing"

	"github.com/nickemma/meridian/internal/consistency"
)

func TestAnalyzeIdentifiesStaleStrongRead(t *testing.T) {
	result := analyze([]record{
		{Kind: "put", Class: "strong", Key: "k", Outcome: "ok", Completed: 10, RaftIndex: 4},
		{Kind: "get", Class: "strong", Key: "k", Outcome: "ok", Invoked: 20, Completed: 30, RaftIndex: 3},
	})
	got := result.Classes["strong"]
	if got.EligibleReads != 1 || got.KnownStaleReads != 1 || got.MaxLagMS != .00002 {
		t.Fatalf("summary = %#v", got)
	}
}

func TestAnalyzeAcceptsDominatingWeakVersion(t *testing.T) {
	vector := consistency.VersionVector{"node": 2}
	encoded, err := vector.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	clock := base64.StdEncoding.EncodeToString(encoded)
	result := analyze([]record{
		{Kind: "put", Class: "causal", Key: "k", Outcome: "ok", Completed: 10, Context: contextRecord{Vector: clock}},
		{Kind: "get", Class: "causal", Key: "k", Outcome: "ok", Invoked: 20, Completed: 30, Versions: []version{{Context: contextRecord{Vector: clock}}}},
	})
	got := result.Classes["causal"]
	if got.EligibleReads != 1 || got.KnownStaleReads != 0 || got.UnknownMetadata != 0 {
		t.Fatalf("summary = %#v", got)
	}
}
