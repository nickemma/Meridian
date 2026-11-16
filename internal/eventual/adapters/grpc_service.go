// Package adapters carries eventual gossip batches between replicas over gRPC.
package adapters

import (
	"context"

	codec "github.com/nickemma/meridian/internal/consistency/adapters"
	consistency "github.com/nickemma/meridian/internal/consistency/domain"
	"github.com/nickemma/meridian/internal/eventual/application"
	pb "github.com/nickemma/meridian/proto/causal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	pb.UnimplementedEventualReplicationServiceServer
	replica *application.Replica
}

func RegisterService(server grpc.ServiceRegistrar, replica *application.Replica) {
	pb.RegisterEventualReplicationServiceServer(server, &Service{replica: replica})
}

func (s *Service) Gossip(_ context.Context, request *pb.GossipRequest) (*pb.GossipResponse, error) {
	records := make([]consistency.Record, 0, len(request.GetRecords()))
	for index, message := range request.GetRecords() {
		record, err := codec.RecordFromProto(message)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "decode gossip record %d: %v", index, err)
		}
		records = append(records, record)
	}
	if err := s.replica.ReceiveAll(records); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "receive gossip batch: %v", err)
	}
	return &pb.GossipResponse{AcceptedRecords: uint32(len(records))}, nil
}
