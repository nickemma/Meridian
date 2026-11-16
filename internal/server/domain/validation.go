package domain

import (
	"time"

	consistency "github.com/nickemma/meridian/internal/consistency/domain"
)

// validateEnvelope checks the fields every request needs.
func validateEnvelope(r Request, now time.Time) error {
	if r.RequestID == "" {
		return Errorf(InvalidArgument, "request_id is required")
	}
	if len(r.Key) == 0 {
		return Errorf(InvalidArgument, "key is required")
	}
	if r.DeadlineUnixNano > 0 && now.UnixNano() >= r.DeadlineUnixNano {
		return Errorf(DeadlineExceeded, "request deadline has elapsed")
	}
	return nil
}

// ValidateStrong admits a request for the strong (Raft) path.
func ValidateStrong(r Request, now time.Time) error {
	if err := validateEnvelope(r, now); err != nil {
		return err
	}
	switch r.Class {
	case consistency.Strong:
		return nil
	case consistency.Causal, consistency.Eventual:
		return Errorf(Unimplemented, "%s consistency path is not implemented", r.Class)
	default:
		return Errorf(InvalidArgument, "consistency must be specified")
	}
}

// ValidateWeak admits a causal or eventual request and decodes its context.
func ValidateWeak(r Request, now time.Time) (Dependencies, error) {
	if err := validateEnvelope(r, now); err != nil {
		return Dependencies{}, err
	}
	dependencies := Dependencies{Vector: consistency.VersionVector{}}
	if r.Context == nil {
		return dependencies, nil
	}
	if len(r.Context.VectorClock) > 0 {
		vector, err := consistency.UnmarshalVersionVector(r.Context.VectorClock)
		if err != nil {
			return Dependencies{}, Errorf(InvalidArgument, "decode causal vector clock: %v", err)
		}
		dependencies.Vector = vector
	}
	dependencies.RaftIndex = r.Context.RaftIndex
	dependencies.PolicyVersion = r.Context.PolicyVersion
	return dependencies, nil
}
