// Package application implements Meridian's client storage use cases: strong
// operations through Raft, and causal and eventual operations through their
// local replicas, all admitted by the same namespace-policy registry.
package application

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	causal "github.com/nickemma/meridian/internal/causal/domain"
	consistency "github.com/nickemma/meridian/internal/consistency/domain"
	consistencyports "github.com/nickemma/meridian/internal/consistency/ports"
	raft "github.com/nickemma/meridian/internal/raft/domain"
	"github.com/nickemma/meridian/internal/server/domain"
	"github.com/nickemma/meridian/internal/server/ports"
)

// Dependencies are the collaborators a KVService orchestrates. Publish,
// Gossip and Observer may be nil.
type Dependencies struct {
	Consensus ports.Consensus
	State     ports.StateReader
	Causal    ports.CausalReplica
	Eventual  ports.EventualReplica
	Policies  consistencyports.PolicyResolver
	Publish   ports.Publish
	Gossip    ports.Gossip
	Observer  ports.DependencyObserver
}

// KVService serializes strong client operations at one leader with strongMu
// until durable request de-duplication and structured apply results are
// implemented. Raft still provides the cross-replica order.
type KVService struct {
	deps     Dependencies
	strongMu sync.Mutex
	now      func() time.Time
}

func NewKVService(deps Dependencies) *KVService {
	return &KVService{deps: deps, now: time.Now}
}

func (s *KVService) Get(ctx context.Context, request domain.Request) (domain.ReadResult, error) {
	ctx, cancel := withRequestDeadline(ctx, request)
	defer cancel()
	switch request.Class {
	case consistency.Causal:
		return s.getCausal(ctx, request)
	case consistency.Eventual:
		return s.getEventual(request)
	}
	if err := s.admitStrong(request, func() error {
		_, err := s.deps.Policies.ValidateRead(string(request.Key), consistency.Strong)
		return err
	}); err != nil {
		return domain.ReadResult{}, err
	}
	s.strongMu.Lock()
	defer s.strongMu.Unlock()
	index, err := s.deps.Consensus.LinearizableBarrier(ctx)
	if err != nil {
		return domain.ReadResult{}, s.consensusError(err)
	}
	value, found, err := s.deps.State.Get(request.Key)
	if err != nil {
		return domain.ReadResult{}, domain.Errorf(domain.Internal, "read durable state machine: %v", err)
	}
	return domain.ReadResult{Found: found, Value: value, Served: consistency.Strong, RaftIndex: index}, nil
}

func (s *KVService) Put(ctx context.Context, request domain.Request) (domain.WriteResult, error) {
	return s.write(ctx, request, false)
}

func (s *KVService) Delete(ctx context.Context, request domain.Request) (domain.WriteResult, error) {
	return s.write(ctx, request, true)
}

// write is the shared Put/Delete flow; tombstone selects delete.
func (s *KVService) write(ctx context.Context, request domain.Request, tombstone bool) (domain.WriteResult, error) {
	ctx, cancel := withRequestDeadline(ctx, request)
	defer cancel()
	switch request.Class {
	case consistency.Causal:
		return s.writeCausal(ctx, request, tombstone)
	case consistency.Eventual:
		return s.writeEventual(request, tombstone)
	}
	if err := s.admitStrongWrite(request); err != nil {
		return domain.WriteResult{}, err
	}
	command, name := raft.Command{Type: raft.CommandPut, Key: request.Key, Value: request.Value}, "put"
	if tombstone {
		command, name = raft.Command{Type: raft.CommandDelete, Key: request.Key}, "delete"
	}
	encoded, err := command.MarshalBinary()
	if err != nil {
		return domain.WriteResult{}, domain.Errorf(domain.InvalidArgument, "encode %s command: %v", name, err)
	}
	s.strongMu.Lock()
	defer s.strongMu.Unlock()
	index, err := s.deps.Consensus.SubmitAndWait(ctx, encoded)
	if err != nil {
		return domain.WriteResult{}, s.consensusError(err)
	}
	return domain.WriteResult{RaftIndex: index, Served: consistency.Strong}, nil
}

// CompareAndSet is strong-only: it reads after a linearizable barrier and
// submits a Put only when the expectation matches.
func (s *KVService) CompareAndSet(ctx context.Context, request domain.Request) (domain.CompareAndSetResult, error) {
	ctx, cancel := withRequestDeadline(ctx, request)
	defer cancel()
	if err := s.admitStrongWrite(request); err != nil {
		return domain.CompareAndSetResult{}, err
	}
	s.strongMu.Lock()
	defer s.strongMu.Unlock()

	barrier, err := s.deps.Consensus.LinearizableBarrier(ctx)
	if err != nil {
		return domain.CompareAndSetResult{}, s.consensusError(err)
	}
	current, exists, err := s.deps.State.Get(request.Key)
	if err != nil {
		return domain.CompareAndSetResult{}, domain.Errorf(domain.Internal, "read compare-and-set key: %v", err)
	}
	if exists != request.ExpectedExists || (exists && !bytes.Equal(current, request.Expected)) {
		return domain.CompareAndSetResult{Swapped: false, CurrentExists: exists, CurrentValue: current, RaftIndex: barrier, Served: consistency.Strong}, nil
	}
	command, err := (raft.Command{Type: raft.CommandPut, Key: request.Key, Value: request.Value}).MarshalBinary()
	if err != nil {
		return domain.CompareAndSetResult{}, domain.Errorf(domain.InvalidArgument, "encode compare-and-set command: %v", err)
	}
	index, err := s.deps.Consensus.SubmitAndWait(ctx, command)
	if err != nil {
		return domain.CompareAndSetResult{}, s.consensusError(err)
	}
	return domain.CompareAndSetResult{Swapped: true, CurrentExists: true, CurrentValue: request.Value, RaftIndex: index, Served: consistency.Strong}, nil
}

// Status reports the local Raft node.
func (s *KVService) Status() raft.Status { return s.deps.Consensus.Status() }

func (s *KVService) getCausal(ctx context.Context, request domain.Request) (domain.ReadResult, error) {
	deps, err := domain.ValidateWeak(request, s.now())
	if err != nil {
		return domain.ReadResult{}, err
	}
	policy, err := s.readablePolicy(request.Key, consistency.Causal)
	if err != nil {
		return domain.ReadResult{}, err
	}
	var snapshot causal.Snapshot
	err = s.waitForDependencies(deps, func() error {
		var readErr error
		snapshot, readErr = s.deps.Causal.ReadContext(ctx, request.Key, deps.Vector, deps.RaftIndex)
		return causalError(readErr)
	})
	if err != nil {
		return domain.ReadResult{}, err
	}
	return visibleResult(consistency.Causal, snapshot.Values, domain.Version{Vector: snapshot.Frontier, RaftIndex: snapshot.AppliedRaftIdx, PolicyVersion: policy.Version}, snapshot.AppliedRaftIdx), nil
}

func (s *KVService) writeCausal(ctx context.Context, request domain.Request, tombstone bool) (domain.WriteResult, error) {
	deps, err := domain.ValidateWeak(request, s.now())
	if err != nil {
		return domain.WriteResult{}, err
	}
	policy, err := s.deps.Policies.ValidateWrite(string(request.Key), consistency.Causal, deps.PolicyVersion)
	if err != nil {
		return domain.WriteResult{}, policyError(err)
	}
	value := request.Value
	if tombstone {
		value = nil
	}
	var record consistency.Record
	err = s.waitForDependencies(deps, func() error {
		var writeErr error
		record, writeErr = s.deps.Causal.WriteContext(ctx, request.Key, value, tombstone, deps.Vector, deps.RaftIndex, policy.Version)
		return causalError(writeErr)
	})
	if err != nil {
		return domain.WriteResult{}, err
	}
	if s.deps.Publish != nil {
		s.deps.Publish(record)
	}
	return domain.WriteResult{Served: consistency.Causal, Context: &domain.Version{Vector: record.Version, RaftIndex: record.RaftIndex, PolicyVersion: policy.Version}}, nil
}

func (s *KVService) getEventual(request domain.Request) (domain.ReadResult, error) {
	// Eventual reads carry no dependency obligation; the context is only
	// validated for well-formedness.
	if _, err := domain.ValidateWeak(request, s.now()); err != nil {
		return domain.ReadResult{}, err
	}
	policy, err := s.readablePolicy(request.Key, consistency.Eventual)
	if err != nil {
		return domain.ReadResult{}, err
	}
	snapshot := s.deps.Eventual.Read(request.Key)
	return visibleResult(consistency.Eventual, snapshot.Values, domain.Version{Vector: snapshot.Frontier, PolicyVersion: policy.Version}, 0), nil
}

func (s *KVService) writeEventual(request domain.Request, tombstone bool) (domain.WriteResult, error) {
	deps, err := domain.ValidateWeak(request, s.now())
	if err != nil {
		return domain.WriteResult{}, err
	}
	policy, err := s.deps.Policies.ValidateWrite(string(request.Key), consistency.Eventual, deps.PolicyVersion)
	if err != nil {
		return domain.WriteResult{}, policyError(err)
	}
	value := request.Value
	if tombstone {
		value = nil
	}
	record, err := s.deps.Eventual.Write(request.Key, value, tombstone, policy.Version)
	if err != nil {
		return domain.WriteResult{}, domain.Errorf(domain.Internal, "eventual replication state: %v", err)
	}
	if s.deps.Gossip != nil {
		s.deps.Gossip([]consistency.Record{record})
	}
	return domain.WriteResult{Served: consistency.Eventual, Context: &domain.Version{Vector: record.Version, PolicyVersion: policy.Version}}, nil
}

// admitStrong validates a strong request and then applies its policy check.
func (s *KVService) admitStrong(request domain.Request, policyCheck func() error) error {
	if err := domain.ValidateStrong(request, s.now()); err != nil {
		return err
	}
	if err := policyCheck(); err != nil {
		return policyError(err)
	}
	return nil
}

func (s *KVService) admitStrongWrite(request domain.Request) error {
	return s.admitStrong(request, func() error {
		_, err := s.deps.Policies.ValidateWrite(string(request.Key), consistency.Strong, request.PolicyVersion())
		return err
	})
}

// readablePolicy admits a weak read and checks that the namespace's records
// are materialized on that path: a namespace is readable by its own class,
// and strong namespaces are also materialized on both weak paths.
func (s *KVService) readablePolicy(key []byte, class consistency.Class) (consistency.NamespacePolicy, error) {
	policy, err := s.deps.Policies.ValidateRead(string(key), class)
	if err != nil {
		return consistency.NamespacePolicy{}, policyError(err)
	}
	if policy.Class != class && policy.Class != consistency.Strong {
		return consistency.NamespacePolicy{}, domain.Errorf(domain.FailedPrecondition, "%s reads are not materialized for %s namespace %s", class, policy.Class, policy.Prefix)
	}
	return policy, nil
}

// waitForDependencies runs a causal operation and, when it initially had to
// wait for dependencies, reports the wait and its final outcome.
func (s *KVService) waitForDependencies(deps domain.Dependencies, operation func() error) error {
	start := s.now()
	waiting := s.deps.Observer != nil && !s.deps.Causal.DependenciesSatisfied(deps.Vector, deps.RaftIndex)
	err := operation()
	if waiting {
		s.deps.Observer.Waited(start, err)
	}
	return err
}

// consensusError keeps the caller's own timeout distinguishable from an
// unavailable or unknown leader.
func (s *KVService) consensusError(err error) error {
	if isContextError(err) {
		return err
	}
	if leader := s.deps.Consensus.Status().LeaderID; leader != "" {
		return domain.Errorf(domain.Unavailable, "leader is %s: %v", leader, err)
	}
	return domain.Errorf(domain.Unavailable, "leader unavailable: %v", err)
}

func causalError(err error) error {
	switch {
	case err == nil || isContextError(err):
		return err
	case errors.Is(err, causal.ErrUnsatisfiedDependencies):
		return domain.Errorf(domain.FailedPrecondition, "causal dependency unavailable locally: %v", err)
	default:
		return domain.Errorf(domain.Internal, "causal replication state: %v", err)
	}
}

func policyError(err error) error {
	return domain.Errorf(domain.FailedPrecondition, "namespace policy: %v", err)
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func withRequestDeadline(ctx context.Context, request domain.Request) (context.Context, context.CancelFunc) {
	if deadline, ok := request.Deadline(); ok {
		return context.WithDeadline(ctx, deadline)
	}
	return ctx, func() {}
}

// visibleResult builds a weak-path read result. A single live sibling is
// also reported as the resolved value.
func visibleResult(class consistency.Class, records []consistency.Record, context domain.Version, raftIndex uint64) domain.ReadResult {
	result := domain.ReadResult{Served: class, Context: &context, RaftIndex: raftIndex, Versions: make([]domain.VersionedValue, 0, len(records))}
	for _, record := range records {
		result.Versions = append(result.Versions, domain.VersionedValue{Value: record.Value, Tombstone: record.Tombstone, Context: domain.Version{Vector: record.Version, RaftIndex: record.RaftIndex, PolicyVersion: context.PolicyVersion}})
	}
	if len(records) == 1 && !records[0].Tombstone {
		result.Found = true
		result.Value = records[0].Value
	}
	return result
}
