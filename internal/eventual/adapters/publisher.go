package adapters

import (
	"context"
	"log"

	codec "github.com/nickemma/meridian/internal/consistency/adapters"
	consistency "github.com/nickemma/meridian/internal/consistency/domain"
	consistencyports "github.com/nickemma/meridian/internal/consistency/ports"
	"github.com/nickemma/meridian/internal/eventual/ports"
	pb "github.com/nickemma/meridian/proto/causal"
	"google.golang.org/grpc"
)

// NewGossip sends a batch asynchronously. The server uses it both after a
// local eventual write and for each periodic full-state anti-entropy round.
func NewGossip(peers []codec.Peer, credentials codec.CredentialsFor, observer consistencyports.ReplicationObserver) ports.Gossip {
	fanOut := codec.NewFanOut(consistency.Eventual.String(), "gossip", peers, credentials, observer)
	return func(records []consistency.Record) {
		if len(records) == 0 {
			return
		}
		messages, err := codec.RecordsToProto(records)
		if err != nil {
			log.Printf("[eventual] encode record for gossip: %v", err)
			return
		}
		fanOut.Send(func(ctx context.Context, connection *grpc.ClientConn) error {
			_, err := pb.NewEventualReplicationServiceClient(connection).Gossip(ctx, &pb.GossipRequest{Records: messages})
			return err
		})
	}
}
