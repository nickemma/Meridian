package adapters

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/nickemma/meridian/internal/causal/application"
	codec "github.com/nickemma/meridian/internal/consistency/adapters"
	consistency "github.com/nickemma/meridian/internal/consistency/domain"
	pb "github.com/nickemma/meridian/proto/causal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func TestCausalReplicationPushPreservesDependencyOrdering(t *testing.T) {
	origin, err := application.NewReplica("origin")
	if err != nil {
		t.Fatal(err)
	}
	first, err := origin.Write([]byte("/causal/a"), []byte("one"), false, nil, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := origin.Write([]byte("/causal/b"), []byte("two"), false, first.Version, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	target, err := application.NewReplica("target")
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{replica: target}

	encodedSecond, err := codec.RecordToProto(second)
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.Push(context.Background(), &pb.PushRequest{Record: encodedSecond})
	if err != nil || response.GetApplied() {
		t.Fatalf("Push(second) = (%#v, %v), want held record", response, err)
	}
	encodedFirst, err := codec.RecordToProto(first)
	if err != nil {
		t.Fatal(err)
	}
	response, err = service.Push(context.Background(), &pb.PushRequest{Record: encodedFirst})
	if err != nil || !response.GetApplied() {
		t.Fatalf("Push(first) = (%#v, %v), want immediate apply", response, err)
	}
	got, err := target.Read([]byte("/causal/b"), consistency.VersionVector{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Values) != 1 || string(got.Values[0].Value) != "two" {
		t.Fatalf("replicated values = %#v", got.Values)
	}
}

func TestCausalReplicationPushOverGRPC(t *testing.T) {
	target, err := application.NewReplica("target")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	RegisterService(grpcServer, target)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	origin, err := application.NewReplica("origin")
	if err != nil {
		t.Fatal(err)
	}
	record, err := origin.Write([]byte("/causal/k"), []byte("value"), false, nil, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Publish through the production publisher and shared fan-out.
	publish := NewPublisher([]codec.Peer{{ID: "target", Address: listener.Addr().String()}},
		func(string) credentials.TransportCredentials { return insecure.NewCredentials() }, nil)
	publish(record)

	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := target.Read([]byte("/causal/k"), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Values) == 1 && string(got.Values[0].Value) == "value" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("replicated values = %#v", got.Values)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
