package adapters

import (
	"context"
	"log"

	codec "github.com/nickemma/meridian/internal/consistency/adapters"
	consistency "github.com/nickemma/meridian/internal/consistency/domain"
	"github.com/nickemma/meridian/internal/consistency/ports"
	pb "github.com/nickemma/meridian/proto/causal"
	"google.golang.org/grpc"
)

// NewPublisher best-effort disseminates a local causal update without making
// client write latency depend on another region. The receiver's idempotent
// Push makes retries safe. There is deliberately no claim of durability or
// anti-entropy here: those require a durable outbox and a reconciliation
// protocol, which are separate implementation gates.
func NewPublisher(peers []codec.Peer, credentials codec.CredentialsFor, observer ports.ReplicationObserver) func(consistency.Record) {
	fanOut := codec.NewFanOut(consistency.Causal.String(), "push", peers, credentials, observer)
	return func(record consistency.Record) {
		message, err := codec.RecordToProto(record)
		if err != nil {
			log.Printf("[causal] encode record for dissemination: %v", err)
			return
		}
		fanOut.Send(func(ctx context.Context, connection *grpc.ClientConn) error {
			_, err := pb.NewCausalReplicationServiceClient(connection).Push(ctx, &pb.PushRequest{Record: message})
			return err
		})
	}
}
