package adapters

import (
	"github.com/nickemma/meridian/internal/consistency/domain"
	kv "github.com/nickemma/meridian/proto/kv"
)

// ClassFromProto maps the wire consistency enum to the domain class. An
// unspecified or unknown value maps to the zero (invalid) class.
func ClassFromProto(c kv.Consistency) domain.Class {
	switch c {
	case kv.Consistency_CONSISTENCY_STRONG:
		return domain.Strong
	case kv.Consistency_CONSISTENCY_CAUSAL:
		return domain.Causal
	case kv.Consistency_CONSISTENCY_EVENTUAL:
		return domain.Eventual
	default:
		return 0
	}
}

// ClassToProto maps a domain class to the wire enum.
func ClassToProto(c domain.Class) kv.Consistency {
	switch c {
	case domain.Strong:
		return kv.Consistency_CONSISTENCY_STRONG
	case domain.Causal:
		return kv.Consistency_CONSISTENCY_CAUSAL
	case domain.Eventual:
		return kv.Consistency_CONSISTENCY_EVENTUAL
	default:
		return kv.Consistency_CONSISTENCY_UNSPECIFIED
	}
}
