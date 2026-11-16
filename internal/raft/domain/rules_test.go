package domain

import "testing"

func TestCandidateLogUpToDate(t *testing.T) {
	for _, test := range []struct {
		candidateTerm, candidateIndex, ourTerm, ourIndex uint64
		want                                             bool
	}{
		{2, 1, 1, 5, true},
		{1, 5, 1, 5, true},
		{1, 4, 1, 5, false},
		{0, 0, 1, 1, false},
	} {
		if got := CandidateLogUpToDate(test.candidateTerm, test.candidateIndex, test.ourTerm, test.ourIndex); got != test.want {
			t.Errorf("CandidateLogUpToDate(%+v) = %t", test, got)
		}
	}
}

func TestQuorumIndex(t *testing.T) {
	if index, ok := QuorumIndex([]uint64{5, 1, 3}, 2); !ok || index != 3 {
		t.Fatalf("QuorumIndex = %d, %t; want 3, true", index, ok)
	}
	if _, ok := QuorumIndex([]uint64{5}, 2); ok {
		t.Fatal("quorum reported with too few indexes")
	}
}

func TestPersistentStateValidation(t *testing.T) {
	if err := (PersistentState{CommitIndex: 1}).Validate(); err == nil {
		t.Fatal("commit beyond log accepted")
	}
	if err := (PersistentState{Log: []LogEntry{{Index: 1, Term: 0}}}).Validate(); err == nil {
		t.Fatal("zero-term entry accepted")
	}
	valid := PersistentState{Log: []LogEntry{{Index: 1, Term: 1}}, CommitIndex: 1, LastApplied: 1}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
}
