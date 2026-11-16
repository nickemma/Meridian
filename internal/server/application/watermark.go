package application

import (
	"fmt"

	consistency "github.com/nickemma/meridian/internal/consistency/domain"
	consistencyports "github.com/nickemma/meridian/internal/consistency/ports"
	raft "github.com/nickemma/meridian/internal/raft/domain"
	raftports "github.com/nickemma/meridian/internal/raft/ports"
	"github.com/nickemma/meridian/internal/server/ports"
)

// RaftWatermark wraps the strong state machine. It advances the causal
// bridge only after local Raft storage applied a committed command, making a
// Raft-index dependency a real local visibility condition instead of a
// replicated assertion. Strong writes are then materialized on both weak paths.
type RaftWatermark struct {
	next     raftports.StateMachine
	causal   ports.CausalMaterializer
	eventual ports.EventualMaterializer
	policies consistencyports.PolicyResolver
}

var _ raftports.StateMachine = RaftWatermark{}

func NewRaftWatermark(next raftports.StateMachine, causal ports.CausalMaterializer, eventual ports.EventualMaterializer, policies consistencyports.PolicyResolver) RaftWatermark {
	return RaftWatermark{next: next, causal: causal, eventual: eventual, policies: policies}
}

func (m RaftWatermark) Apply(index uint64, command []byte) error {
	if err := m.next.Apply(index, command); err != nil {
		return err
	}
	if err := m.causal.ObserveRaft(index); err != nil {
		return err
	}
	decoded, err := raft.UnmarshalCommand(command)
	if err != nil || (decoded.Type != raft.CommandPut && decoded.Type != raft.CommandDelete) {
		// Noops and unsupported command types do not change a user key. The
		// KV API currently implements CAS as a checked Raft Put, so no CAS
		// result is lost here.
		return nil
	}
	policy, err := m.policies.Lookup(string(decoded.Key))
	if err != nil || policy.Class != consistency.Strong {
		return nil
	}
	record := consistency.Record{
		Key:           decoded.Key,
		Value:         decoded.Value,
		Tombstone:     decoded.Type == raft.CommandDelete,
		Version:       consistency.VersionVector{"raft": index},
		Dependencies:  consistency.VersionVector{},
		Origin:        "raft",
		RaftIndex:     index,
		PolicyVersion: policy.Version,
	}
	if _, err := m.causal.Receive(record); err != nil {
		return fmt.Errorf("materialize strong record for causal path: %w", err)
	}
	if err := m.eventual.Receive(record); err != nil {
		return fmt.Errorf("materialize strong record for eventual path: %w", err)
	}
	return nil
}
