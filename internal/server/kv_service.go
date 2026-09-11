package server

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nickemma/meridian/internal/causal"
	"github.com/nickemma/meridian/internal/consistency"
	"github.com/nickemma/meridian/internal/eventual"
	"github.com/nickemma/meridian/internal/raft"
	kv "github.com/nickemma/meridian/proto/kv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// kvService is the client-visible strong KV path. strongMu serializes client
// operations at one leader until durable request de-duplication and structured
// apply results are implemented. Raft still provides the cross-replica order.
type kvService struct {
	kv.UnimplementedKVServiceServer
	node     *raft.Node
	store    raft.KVStore
	causal   *causal.Replica
	eventual *eventual.Replica
	policies *consistency.PolicyRegistry
	publish  func(consistency.Record)
	gossip   func([]consistency.Record)
	strongMu sync.Mutex
}

func registerKVService(server grpc.ServiceRegistrar, node *raft.Node, store raft.KVStore, causalReplica *causal.Replica, eventualReplica *eventual.Replica, policies *consistency.PolicyRegistry, publish func(consistency.Record), gossip func([]consistency.Record)) {
	kv.RegisterKVServiceServer(server, &kvService{node: node, store: store, causal: causalReplica, eventual: eventualReplica, policies: policies, publish: publish, gossip: gossip})
}

func (s *kvService) Get(ctx context.Context, request *kv.GetRequest) (*kv.GetResponse, error) {
	if request.GetConsistency() == kv.Consistency_CONSISTENCY_CAUSAL {
		return s.getCausal(request)
	}
	if request.GetConsistency() == kv.Consistency_CONSISTENCY_EVENTUAL {
		return s.getEventual(request)
	}
	if err := validateStrongRequest(request.GetRequestId(), request.GetKey(), request.GetConsistency(), request.GetDeadlineUnixNano()); err != nil {
		return nil, err
	}
	if err := s.validatePolicyRead(request.GetKey(), consistency.Strong); err != nil {
		return nil, err
	}
	s.strongMu.Lock()
	defer s.strongMu.Unlock()
	index, err := s.node.LinearizableBarrier(ctx)
	if err != nil {
		return nil, rpcError(s.node, err)
	}
	value, found, err := s.store.Get(request.GetKey())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read durable state machine: %v", err)
	}
	return &kv.GetResponse{Found: found, Value: value, ServedConsistency: kv.Consistency_CONSISTENCY_STRONG, RaftIndex: index}, nil
}

func (s *kvService) Put(ctx context.Context, request *kv.PutRequest) (*kv.PutResponse, error) {
	if request.GetConsistency() == kv.Consistency_CONSISTENCY_CAUSAL {
		return s.putCausal(request)
	}
	if request.GetConsistency() == kv.Consistency_CONSISTENCY_EVENTUAL {
		return s.putEventual(request)
	}
	if err := validateStrongRequest(request.GetRequestId(), request.GetKey(), request.GetConsistency(), request.GetDeadlineUnixNano()); err != nil {
		return nil, err
	}
	if err := s.validatePolicyWrite(request.GetKey(), consistency.Strong, policyVersion(request.GetContext())); err != nil {
		return nil, err
	}
	command, err := (raft.Command{Type: raft.CommandPut, Key: request.GetKey(), Value: request.GetValue()}).MarshalBinary()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "encode put command: %v", err)
	}
	s.strongMu.Lock()
	defer s.strongMu.Unlock()
	index, err := s.node.SubmitAndWait(ctx, command)
	if err != nil {
		return nil, rpcError(s.node, err)
	}
	return &kv.PutResponse{RaftIndex: index, ServedConsistency: kv.Consistency_CONSISTENCY_STRONG}, nil
}

func (s *kvService) Delete(ctx context.Context, request *kv.DeleteRequest) (*kv.DeleteResponse, error) {
	if request.GetConsistency() == kv.Consistency_CONSISTENCY_CAUSAL {
		return s.deleteCausal(request)
	}
	if request.GetConsistency() == kv.Consistency_CONSISTENCY_EVENTUAL {
		return s.deleteEventual(request)
	}
	if err := validateStrongRequest(request.GetRequestId(), request.GetKey(), request.GetConsistency(), request.GetDeadlineUnixNano()); err != nil {
		return nil, err
	}
	if err := s.validatePolicyWrite(request.GetKey(), consistency.Strong, policyVersion(request.GetContext())); err != nil {
		return nil, err
	}
	command, err := (raft.Command{Type: raft.CommandDelete, Key: request.GetKey()}).MarshalBinary()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "encode delete command: %v", err)
	}
	s.strongMu.Lock()
	defer s.strongMu.Unlock()
	index, err := s.node.SubmitAndWait(ctx, command)
	if err != nil {
		return nil, rpcError(s.node, err)
	}
	return &kv.DeleteResponse{RaftIndex: index, ServedConsistency: kv.Consistency_CONSISTENCY_STRONG}, nil
}

func (s *kvService) CompareAndSet(ctx context.Context, request *kv.CompareAndSetRequest) (*kv.CompareAndSetResponse, error) {
	if err := validateStrongRequest(request.GetRequestId(), request.GetKey(), request.GetConsistency(), request.GetDeadlineUnixNano()); err != nil {
		return nil, err
	}
	if err := s.validatePolicyWrite(request.GetKey(), consistency.Strong, policyVersion(request.GetContext())); err != nil {
		return nil, err
	}
	s.strongMu.Lock()
	defer s.strongMu.Unlock()

	barrier, err := s.node.LinearizableBarrier(ctx)
	if err != nil {
		return nil, rpcError(s.node, err)
	}
	current, exists, err := s.store.Get(request.GetKey())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read compare-and-set key: %v", err)
	}
	if exists != request.GetExpectedExists() || (exists && !bytes.Equal(current, request.GetExpectedValue())) {
		return &kv.CompareAndSetResponse{Swapped: false, CurrentExists: exists, CurrentValue: current, RaftIndex: barrier, ServedConsistency: kv.Consistency_CONSISTENCY_STRONG}, nil
	}

	command, err := (raft.Command{Type: raft.CommandPut, Key: request.GetKey(), Value: request.GetValue()}).MarshalBinary()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "encode compare-and-set command: %v", err)
	}
	index, err := s.node.SubmitAndWait(ctx, command)
	if err != nil {
		return nil, rpcError(s.node, err)
	}
	return &kv.CompareAndSetResponse{Swapped: true, CurrentExists: true, CurrentValue: request.GetValue(), RaftIndex: index, ServedConsistency: kv.Consistency_CONSISTENCY_STRONG}, nil
}

func (s *kvService) getCausal(request *kv.GetRequest) (*kv.GetResponse, error) {
	context, err := validateCausalRequest(request.GetRequestId(), request.GetKey(), request.GetContext(), request.GetDeadlineUnixNano())
	if err != nil {
		return nil, err
	}
	policy, err := s.policies.ValidateRead(string(request.GetKey()), consistency.Causal)
	if err != nil {
		return nil, policyError(err)
	}
	if policy.Class != consistency.Causal && policy.Class != consistency.Strong {
		return nil, status.Errorf(codes.FailedPrecondition, "causal reads are not materialized for %s namespace %s", policy.Class, policy.Prefix)
	}
	snapshot, err := s.causal.Read(request.GetKey(), context.Vector, context.RaftIndex)
	if err != nil {
		return nil, causalError(err)
	}
	response := &kv.GetResponse{
		ServedConsistency: kv.Consistency_CONSISTENCY_CAUSAL,
		Context:           causalContext(snapshot.Frontier, snapshot.AppliedRaftIdx, policy.Version),
		RaftIndex:         snapshot.AppliedRaftIdx,
		Versions:          versionedValues(snapshot.Values, policy.Version),
	}
	if len(snapshot.Values) == 1 && !snapshot.Values[0].Tombstone {
		response.Found = true
		response.Value = snapshot.Values[0].Value
	}
	return response, nil
}

func (s *kvService) putCausal(request *kv.PutRequest) (*kv.PutResponse, error) {
	context, err := validateCausalRequest(request.GetRequestId(), request.GetKey(), request.GetContext(), request.GetDeadlineUnixNano())
	if err != nil {
		return nil, err
	}
	policy, err := s.policies.ValidateWrite(string(request.GetKey()), consistency.Causal, context.PolicyVersion)
	if err != nil {
		return nil, policyError(err)
	}
	record, err := s.causal.Write(request.GetKey(), request.GetValue(), false, context.Vector, context.RaftIndex, policy.Version)
	if err != nil {
		return nil, causalError(err)
	}
	if s.publish != nil {
		s.publish(record)
	}
	return &kv.PutResponse{ServedConsistency: kv.Consistency_CONSISTENCY_CAUSAL, Context: causalContext(record.Version, record.RaftIndex, policy.Version)}, nil
}

func (s *kvService) deleteCausal(request *kv.DeleteRequest) (*kv.DeleteResponse, error) {
	context, err := validateCausalRequest(request.GetRequestId(), request.GetKey(), request.GetContext(), request.GetDeadlineUnixNano())
	if err != nil {
		return nil, err
	}
	policy, err := s.policies.ValidateWrite(string(request.GetKey()), consistency.Causal, context.PolicyVersion)
	if err != nil {
		return nil, policyError(err)
	}
	record, err := s.causal.Write(request.GetKey(), nil, true, context.Vector, context.RaftIndex, policy.Version)
	if err != nil {
		return nil, causalError(err)
	}
	if s.publish != nil {
		s.publish(record)
	}
	return &kv.DeleteResponse{ServedConsistency: kv.Consistency_CONSISTENCY_CAUSAL, Context: causalContext(record.Version, record.RaftIndex, policy.Version)}, nil
}

func (s *kvService) getEventual(request *kv.GetRequest) (*kv.GetResponse, error) {
	context, err := validateCausalRequest(request.GetRequestId(), request.GetKey(), request.GetContext(), request.GetDeadlineUnixNano())
	if err != nil {
		return nil, err
	}
	policy, err := s.policies.ValidateRead(string(request.GetKey()), consistency.Eventual)
	if err != nil {
		return nil, policyError(err)
	}
	if policy.Class != consistency.Eventual && policy.Class != consistency.Strong {
		return nil, status.Errorf(codes.FailedPrecondition, "eventual reads are not materialized for %s namespace %s", policy.Class, policy.Prefix)
	}
	_ = context // eventual reads carry no dependency obligation.
	snapshot := s.eventual.Read(request.GetKey())
	response := &kv.GetResponse{
		ServedConsistency: kv.Consistency_CONSISTENCY_EVENTUAL,
		Context:           causalContext(snapshot.Frontier, 0, policy.Version),
		Versions:          versionedValues(snapshot.Values, policy.Version),
	}
	if len(snapshot.Values) == 1 && !snapshot.Values[0].Tombstone {
		response.Found = true
		response.Value = snapshot.Values[0].Value
	}
	return response, nil
}

func (s *kvService) putEventual(request *kv.PutRequest) (*kv.PutResponse, error) {
	context, err := validateCausalRequest(request.GetRequestId(), request.GetKey(), request.GetContext(), request.GetDeadlineUnixNano())
	if err != nil {
		return nil, err
	}
	policy, err := s.policies.ValidateWrite(string(request.GetKey()), consistency.Eventual, context.PolicyVersion)
	if err != nil {
		return nil, policyError(err)
	}
	record, err := s.eventual.Write(request.GetKey(), request.GetValue(), false, policy.Version)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "eventual replication state: %v", err)
	}
	if s.gossip != nil {
		s.gossip([]consistency.Record{record})
	}
	return &kv.PutResponse{ServedConsistency: kv.Consistency_CONSISTENCY_EVENTUAL, Context: causalContext(record.Version, 0, policy.Version)}, nil
}

func (s *kvService) deleteEventual(request *kv.DeleteRequest) (*kv.DeleteResponse, error) {
	context, err := validateCausalRequest(request.GetRequestId(), request.GetKey(), request.GetContext(), request.GetDeadlineUnixNano())
	if err != nil {
		return nil, err
	}
	policy, err := s.policies.ValidateWrite(string(request.GetKey()), consistency.Eventual, context.PolicyVersion)
	if err != nil {
		return nil, policyError(err)
	}
	record, err := s.eventual.Write(request.GetKey(), nil, true, policy.Version)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "eventual replication state: %v", err)
	}
	if s.gossip != nil {
		s.gossip([]consistency.Record{record})
	}
	return &kv.DeleteResponse{ServedConsistency: kv.Consistency_CONSISTENCY_EVENTUAL, Context: causalContext(record.Version, 0, policy.Version)}, nil
}

type decodedCausalContext struct {
	Vector        consistency.VersionVector
	RaftIndex     uint64
	PolicyVersion uint64
}

func validateCausalRequest(requestID string, key []byte, supplied *kv.CausalContext, deadlineUnixNano int64) (decodedCausalContext, error) {
	if requestID == "" {
		return decodedCausalContext{}, status.Error(codes.InvalidArgument, "request_id is required")
	}
	if len(key) == 0 {
		return decodedCausalContext{}, status.Error(codes.InvalidArgument, "key is required")
	}
	if deadlineUnixNano > 0 && time.Now().UnixNano() >= deadlineUnixNano {
		return decodedCausalContext{}, status.Error(codes.DeadlineExceeded, "request deadline has elapsed")
	}
	context := decodedCausalContext{Vector: consistency.VersionVector{}}
	if supplied == nil {
		return context, nil
	}
	if len(supplied.GetVectorClock()) > 0 {
		vector, err := consistency.UnmarshalVersionVector(supplied.GetVectorClock())
		if err != nil {
			return decodedCausalContext{}, status.Errorf(codes.InvalidArgument, "decode causal vector clock: %v", err)
		}
		context.Vector = vector
	}
	context.RaftIndex = supplied.GetRaftIndex()
	context.PolicyVersion = supplied.GetPolicyVersion()
	return context, nil
}

func causalContext(vector consistency.VersionVector, raftIndex, policyVersion uint64) *kv.CausalContext {
	encoded, err := vector.MarshalBinary()
	if err != nil {
		// Every vector in a replica passed validation before it became state.
		// An empty response is safer than emitting an unstable context if an
		// internal invariant is ever violated.
		return &kv.CausalContext{RaftIndex: raftIndex, PolicyVersion: policyVersion}
	}
	return &kv.CausalContext{VectorClock: encoded, RaftIndex: raftIndex, PolicyVersion: policyVersion}
}

func versionedValues(records []consistency.Record, policyVersion uint64) []*kv.VersionedValue {
	values := make([]*kv.VersionedValue, 0, len(records))
	for _, record := range records {
		values = append(values, &kv.VersionedValue{Value: record.Value, Tombstone: record.Tombstone, Context: causalContext(record.Version, record.RaftIndex, policyVersion)})
	}
	return values
}

func (s *kvService) validatePolicyWrite(key []byte, class consistency.Class, version uint64) error {
	if _, err := s.policies.ValidateWrite(string(key), class, version); err != nil {
		return policyError(err)
	}
	return nil
}

func (s *kvService) validatePolicyRead(key []byte, class consistency.Class) error {
	if _, err := s.policies.ValidateRead(string(key), class); err != nil {
		return policyError(err)
	}
	return nil
}

func policyVersion(context *kv.CausalContext) uint64 {
	if context == nil {
		return 0
	}
	return context.GetPolicyVersion()
}

func policyError(err error) error {
	return status.Errorf(codes.FailedPrecondition, "namespace policy: %v", err)
}

func causalError(err error) error {
	if errors.Is(err, causal.ErrUnsatisfiedDependencies) {
		return status.Errorf(codes.FailedPrecondition, "causal dependency unavailable locally: %v", err)
	}
	return status.Errorf(codes.Internal, "causal replication state: %v", err)
}

func (s *kvService) Status(context.Context, *kv.StatusRequest) (*kv.StatusResponse, error) {
	state := s.node.Status()
	return &kv.StatusResponse{
		NodeId: state.NodeID, Role: state.Role, Term: state.Term, LeaderId: state.LeaderID,
		CommitIndex: state.CommitIndex, LastApplied: state.LastApplied, StorageHealthy: true,
	}, nil
}

func validateStrongRequest(requestID string, key []byte, consistency kv.Consistency, deadlineUnixNano int64) error {
	if requestID == "" {
		return status.Error(codes.InvalidArgument, "request_id is required")
	}
	if len(key) == 0 {
		return status.Error(codes.InvalidArgument, "key is required")
	}
	if deadlineUnixNano > 0 && time.Now().UnixNano() >= deadlineUnixNano {
		return status.Error(codes.DeadlineExceeded, "request deadline has elapsed")
	}
	switch consistency {
	case kv.Consistency_CONSISTENCY_STRONG:
		return nil
	case kv.Consistency_CONSISTENCY_CAUSAL, kv.Consistency_CONSISTENCY_EVENTUAL:
		return status.Errorf(codes.Unimplemented, "%s consistency path is not implemented", consistency)
	default:
		return status.Error(codes.InvalidArgument, "consistency must be specified")
	}
}

func rpcError(node *raft.Node, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	state := node.Status()
	if state.LeaderID != "" {
		return status.Errorf(codes.Unavailable, "leader is %s: %v", state.LeaderID, err)
	}
	return status.Errorf(codes.Unavailable, "leader unavailable: %v", err)
}
