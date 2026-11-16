// Package adapters contains the transport-facing pieces shared by the causal
// and eventual replication paths: the protobuf record codec and the outbound
// peer fan-out.
package adapters

import (
	"fmt"

	"github.com/nickemma/meridian/internal/consistency/domain"
	pb "github.com/nickemma/meridian/proto/causal"
)

// RecordFromProto decodes and structurally validates a replicated record's
// vector fields. Semantic admission stays with the receiving replica.
func RecordFromProto(message *pb.ReplicatedRecord) (domain.Record, error) {
	version, err := domain.UnmarshalVersionVector(message.GetVersionVector())
	if err != nil {
		return domain.Record{}, fmt.Errorf("version vector: %w", err)
	}
	var dependencies domain.VersionVector
	if encoded := message.GetDependencies(); len(encoded) > 0 {
		dependencies, err = domain.UnmarshalVersionVector(encoded)
		if err != nil {
			return domain.Record{}, fmt.Errorf("dependencies: %w", err)
		}
	}
	return domain.Record{
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

func RecordToProto(record domain.Record) (*pb.ReplicatedRecord, error) {
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

// RecordsToProto encodes a batch, failing on the first malformed record.
func RecordsToProto(records []domain.Record) ([]*pb.ReplicatedRecord, error) {
	messages := make([]*pb.ReplicatedRecord, 0, len(records))
	for _, record := range records {
		message, err := RecordToProto(record)
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, nil
}
