// Package adapters carries causal records between replicas over gRPC.
package adapters

import (
	"context"

	"github.com/nickemma/meridian/internal/causal/application"
	codec "github.com/nickemma/meridian/internal/consistency/adapters"
	pb "github.com/nickemma/meridian/proto/causal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service accepts records sent between replicas. It has no authority to
// choose a value: Replica.Receive either makes a record visible after all
// dependencies are present or holds it in the dependency queue.
type Service struct {
	pb.UnimplementedCausalReplicationServiceServer
	replica *application.Replica
}

func RegisterService(server grpc.ServiceRegistrar, replica *application.Replica) {
	pb.RegisterCausalReplicationServiceServer(server, &Service{replica: replica})
}

func (s *Service) Push(_ context.Context, request *pb.PushRequest) (*pb.PushResponse, error) {
	if request.GetRecord() == nil {
		return nil, status.Error(codes.InvalidArgument, "replication record is required")
	}
	record, err := codec.RecordFromProto(request.GetRecord())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "decode replication record: %v", err)
	}
	applied, err := s.replica.Receive(record)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "receive replication record: %v", err)
	}
	return &pb.PushResponse{Applied: applied}, nil
}
