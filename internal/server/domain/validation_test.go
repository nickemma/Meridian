package domain

import (
	"errors"
	"testing"
	"time"

	consistency "github.com/nickemma/meridian/internal/consistency/domain"
)

func kindOf(err error) ErrorKind {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return 0
}

func TestValidateStrongClassifiesFailures(t *testing.T) {
	now := time.Now()
	valid := Request{RequestID: "r", Key: []byte("/k"), Class: consistency.Strong}
	if err := ValidateStrong(valid, now); err != nil {
		t.Fatal(err)
	}
	for name, test := range map[string]struct {
		mutate func(*Request)
		want   ErrorKind
	}{
		"missing id":  {func(r *Request) { r.RequestID = "" }, InvalidArgument},
		"missing key": {func(r *Request) { r.Key = nil }, InvalidArgument},
		"expired":     {func(r *Request) { r.DeadlineUnixNano = now.Add(-time.Second).UnixNano() }, DeadlineExceeded},
		"causal":      {func(r *Request) { r.Class = consistency.Causal }, Unimplemented},
		"unspecified": {func(r *Request) { r.Class = 0 }, InvalidArgument},
	} {
		request := valid
		test.mutate(&request)
		if got := kindOf(ValidateStrong(request, now)); got != test.want {
			t.Errorf("%s: kind = %d, want %d", name, got, test.want)
		}
	}
}

func TestValidateWeakDecodesContext(t *testing.T) {
	vector, err := consistency.VersionVector{"a": 2}.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	deps, err := ValidateWeak(Request{RequestID: "r", Key: []byte("/k"), Context: &CausalContext{VectorClock: vector, RaftIndex: 3, PolicyVersion: 2}}, time.Now())
	if err != nil || deps.Vector["a"] != 2 || deps.RaftIndex != 3 || deps.PolicyVersion != 2 {
		t.Fatalf("deps = %+v, %v", deps, err)
	}
	if _, err := ValidateWeak(Request{RequestID: "r", Key: []byte("/k"), Context: &CausalContext{VectorClock: []byte{0}}}, time.Now()); kindOf(err) != InvalidArgument {
		t.Fatalf("malformed vector error = %v", err)
	}
}
