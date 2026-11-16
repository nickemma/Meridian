package adapters

import (
	"context"

	codec "github.com/nickemma/meridian/internal/consistency/adapters"
	"github.com/nickemma/meridian/internal/server/application"
	"github.com/nickemma/meridian/internal/server/domain"
	kv "github.com/nickemma/meridian/proto/kv"
	"google.golang.org/grpc"
)

// kvService is the client-facing gRPC KV API. It only translates between
// protobuf and the transport-neutral use cases.
type kvService struct {
	kv.UnimplementedKVServiceServer
	service *application.KVService
}

func registerKVService(server grpc.ServiceRegistrar, service *application.KVService) {
	kv.RegisterKVServiceServer(server, &kvService{service: service})
}

func (s *kvService) Get(ctx context.Context, r *kv.GetRequest) (*kv.GetResponse, error) {
	result, err := s.service.Get(ctx, domain.Request{
		RequestID: r.GetRequestId(), Key: r.GetKey(), Class: codec.ClassFromProto(r.GetConsistency()),
		Context: contextFromProto(r.GetContext()), DeadlineUnixNano: r.GetDeadlineUnixNano(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &kv.GetResponse{
		Found: result.Found, Value: result.Value, ServedConsistency: codec.ClassToProto(result.Served),
		RaftIndex: result.RaftIndex, Context: versionToProto(result.Context), Versions: versionsToProto(result.Versions),
	}, nil
}

func (s *kvService) Put(ctx context.Context, r *kv.PutRequest) (*kv.PutResponse, error) {
	result, err := s.service.Put(ctx, domain.Request{
		RequestID: r.GetRequestId(), Key: r.GetKey(), Value: r.GetValue(), Class: codec.ClassFromProto(r.GetConsistency()),
		Context: contextFromProto(r.GetContext()), DeadlineUnixNano: r.GetDeadlineUnixNano(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &kv.PutResponse{RaftIndex: result.RaftIndex, ServedConsistency: codec.ClassToProto(result.Served), Context: versionToProto(result.Context)}, nil
}

func (s *kvService) Delete(ctx context.Context, r *kv.DeleteRequest) (*kv.DeleteResponse, error) {
	result, err := s.service.Delete(ctx, domain.Request{
		RequestID: r.GetRequestId(), Key: r.GetKey(), Class: codec.ClassFromProto(r.GetConsistency()),
		Context: contextFromProto(r.GetContext()), DeadlineUnixNano: r.GetDeadlineUnixNano(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &kv.DeleteResponse{RaftIndex: result.RaftIndex, ServedConsistency: codec.ClassToProto(result.Served), Context: versionToProto(result.Context)}, nil
}

func (s *kvService) CompareAndSet(ctx context.Context, r *kv.CompareAndSetRequest) (*kv.CompareAndSetResponse, error) {
	result, err := s.service.CompareAndSet(ctx, domain.Request{
		RequestID: r.GetRequestId(), Key: r.GetKey(), Value: r.GetValue(), Class: codec.ClassFromProto(r.GetConsistency()),
		Context: contextFromProto(r.GetContext()), DeadlineUnixNano: r.GetDeadlineUnixNano(),
		ExpectedExists: r.GetExpectedExists(), Expected: r.GetExpectedValue(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &kv.CompareAndSetResponse{
		Swapped: result.Swapped, CurrentExists: result.CurrentExists, CurrentValue: result.CurrentValue,
		RaftIndex: result.RaftIndex, ServedConsistency: codec.ClassToProto(result.Served),
	}, nil
}

func (s *kvService) Status(context.Context, *kv.StatusRequest) (*kv.StatusResponse, error) {
	state := s.service.Status()
	return &kv.StatusResponse{
		NodeId: state.NodeID, Role: state.Role, Term: state.Term, LeaderId: state.LeaderID,
		CommitIndex: state.CommitIndex, LastApplied: state.LastApplied, StorageHealthy: true,
	}, nil
}

func contextFromProto(c *kv.CausalContext) *domain.CausalContext {
	if c == nil {
		return nil
	}
	return &domain.CausalContext{VectorClock: c.GetVectorClock(), RaftIndex: c.GetRaftIndex(), PolicyVersion: c.GetPolicyVersion()}
}

// versionToProto encodes a result context. Every vector in a replica passed
// validation before it became state; if an internal invariant is ever
// violated, an empty vector is safer than an unstable context.
func versionToProto(v *domain.Version) *kv.CausalContext {
	if v == nil {
		return nil
	}
	encoded, err := v.Vector.MarshalBinary()
	if err != nil {
		return &kv.CausalContext{RaftIndex: v.RaftIndex, PolicyVersion: v.PolicyVersion}
	}
	return &kv.CausalContext{VectorClock: encoded, RaftIndex: v.RaftIndex, PolicyVersion: v.PolicyVersion}
}

func versionsToProto(values []domain.VersionedValue) []*kv.VersionedValue {
	if values == nil {
		return nil
	}
	messages := make([]*kv.VersionedValue, 0, len(values))
	for _, value := range values {
		messages = append(messages, &kv.VersionedValue{Value: value.Value, Tombstone: value.Tombstone, Context: versionToProto(&value.Context)})
	}
	return messages
}
