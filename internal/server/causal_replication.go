package server

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nickemma/meridian/internal/causal"
	"github.com/nickemma/meridian/internal/config"
	"github.com/nickemma/meridian/internal/consistency"
	"github.com/nickemma/meridian/internal/eventual"
	"github.com/nickemma/meridian/internal/raft"
	pb "github.com/nickemma/meridian/proto/causal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// causalReplicationService accepts records sent between replicas. It has no
// authority to choose a value: Replica.Receive either makes a record visible
// after all dependencies are present or holds it in the dependency queue.
type causalReplicationService struct {
	pb.UnimplementedCausalReplicationServiceServer
	replica *causal.Replica
}

func registerCausalReplicationService(server grpc.ServiceRegistrar, replica *causal.Replica) {
	pb.RegisterCausalReplicationServiceServer(server, &causalReplicationService{replica: replica})
}

func (s *causalReplicationService) Push(_ context.Context, request *pb.PushRequest) (*pb.PushResponse, error) {
	if request.GetRecord() == nil {
		return nil, status.Error(codes.InvalidArgument, "replication record is required")
	}
	record, err := recordFromProto(request.GetRecord())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "decode replication record: %v", err)
	}
	applied, err := s.replica.Receive(record)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "receive replication record: %v", err)
	}
	return &pb.PushResponse{Applied: applied}, nil
}

type eventualReplicationService struct {
	pb.UnimplementedEventualReplicationServiceServer
	replica *eventual.Replica
}

func registerEventualReplicationService(server grpc.ServiceRegistrar, replica *eventual.Replica) {
	pb.RegisterEventualReplicationServiceServer(server, &eventualReplicationService{replica: replica})
}

func (s *eventualReplicationService) Gossip(_ context.Context, request *pb.GossipRequest) (*pb.GossipResponse, error) {
	records := make([]consistency.Record, 0, len(request.GetRecords()))
	for index, message := range request.GetRecords() {
		record, err := recordFromProto(message)
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

func recordFromProto(message *pb.ReplicatedRecord) (consistency.Record, error) {
	version, err := consistency.UnmarshalVersionVector(message.GetVersionVector())
	if err != nil {
		return consistency.Record{}, fmt.Errorf("version vector: %w", err)
	}
	var dependencies consistency.VersionVector
	if encoded := message.GetDependencies(); len(encoded) > 0 {
		dependencies, err = consistency.UnmarshalVersionVector(encoded)
		if err != nil {
			return consistency.Record{}, fmt.Errorf("dependencies: %w", err)
		}
	}
	return consistency.Record{
		Key:           message.GetKey(),
		Value:         message.GetValue(),
		Tombstone:     message.GetTombstone(),
		Version:       version,
		Dependencies:  dependencies,
		Origin:        message.GetOrigin(),
		RaftIndex:     message.GetRaftIndex(),
		PolicyVersion: message.GetPolicyVersion(),
	}, nil
}

func recordToProto(record consistency.Record) (*pb.ReplicatedRecord, error) {
	version, err := record.Version.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("version vector: %w", err)
	}
	dependencies, err := record.Dependencies.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("dependencies: %w", err)
	}
	return &pb.ReplicatedRecord{
		Key:           record.Key,
		Value:         record.Value,
		Tombstone:     record.Tombstone,
		VersionVector: version,
		Dependencies:  dependencies,
		Origin:        record.Origin,
		RaftIndex:     record.RaftIndex,
		PolicyVersion: record.PolicyVersion,
	}, nil
}

// newCausalPublisher best-effort disseminates a local causal update without
// making client write latency depend on another region. The receiver's
// idempotent Push operation makes retries safe. There is deliberately no claim
// of durability or anti-entropy here: those require a durable outbox and a
// reconciliation protocol, which are separate implementation gates.
func newCausalPublisher(peers []config.Peer) func(consistency.Record) {
	peers = append([]config.Peer(nil), peers...)
	return func(record consistency.Record) {
		message, err := recordToProto(record)
		if err != nil {
			log.Printf("[causal] encode record for dissemination: %v", err)
			return
		}
		for _, peer := range peers {
			peer := peer
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				defer cancel()
				connection, err := grpc.DialContext(ctx, peer.Address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
				if err != nil {
					log.Printf("[causal] connect to %s: %v", peer.ID, err)
					return
				}
				defer connection.Close()
				if _, err := pb.NewCausalReplicationServiceClient(connection).Push(ctx, &pb.PushRequest{Record: message}); err != nil {
					log.Printf("[causal] push to %s: %v", peer.ID, err)
				}
			}()
		}
	}
}

// newEventualPublisher sends a batch asynchronously. The server invokes it
// both after a local eventual write and on a periodic full-state round; the
// latter is the anti-entropy mechanism that recovers a missed initial push.
func newEventualPublisher(peers []config.Peer) func([]consistency.Record) {
	peers = append([]config.Peer(nil), peers...)
	return func(records []consistency.Record) {
		if len(records) == 0 {
			return
		}
		messages := make([]*pb.ReplicatedRecord, 0, len(records))
		for _, record := range records {
			message, err := recordToProto(record)
			if err != nil {
				log.Printf("[eventual] encode record for gossip: %v", err)
				return
			}
			messages = append(messages, message)
		}
		for _, peer := range peers {
			peer := peer
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				defer cancel()
				connection, err := grpc.DialContext(ctx, peer.Address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
				if err != nil {
					log.Printf("[eventual] connect to %s: %v", peer.ID, err)
					return
				}
				defer connection.Close()
				if _, err := pb.NewEventualReplicationServiceClient(connection).Gossip(ctx, &pb.GossipRequest{Records: messages}); err != nil {
					log.Printf("[eventual] gossip to %s: %v", peer.ID, err)
				}
			}()
		}
	}
}

// raftWatermarkStateMachine only advances the causal bridge after local Raft
// storage applied a committed command. This makes a Raft-index dependency a
// real local visibility condition instead of a replicated assertion.
type raftWatermarkStateMachine struct {
	next     raft.StateMachine
	causal   *causal.Replica
	eventual *eventual.Replica
	policies *consistency.PolicyRegistry
}

func (m raftWatermarkStateMachine) Apply(index uint64, command []byte) error {
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
