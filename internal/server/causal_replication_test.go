package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/nickemma/meridian/internal/causal"
	"github.com/nickemma/meridian/internal/consistency"
	pb "github.com/nickemma/meridian/proto/causal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestCausalReplicationPushPreservesDependencyOrdering(t *testing.T) {
	origin, err := causal.NewReplica("origin")
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
	target, err := causal.NewReplica("target")
	if err != nil {
		t.Fatal(err)
	}
	service := &causalReplicationService{replica: target}

	encodedSecond, err := recordToProto(second)
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.Push(context.Background(), &pb.PushRequest{Record: encodedSecond})
	if err != nil || response.GetApplied() {
		t.Fatalf("Push(second) = (%#v, %v), want held record", response, err)
	}
	encodedFirst, err := recordToProto(first)
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

func TestCausalRecordProtoRoundTrip(t *testing.T) {
	record := consistency.Record{
		Key:           []byte("/causal/k"),
		Value:         []byte("v"),
		Version:       consistency.VersionVector{"a": 2, "b": 1},
		Dependencies:  consistency.VersionVector{"a": 1},
		Origin:        "a",
		RaftIndex:     4,
		PolicyVersion: 3,
	}
	message, err := recordToProto(record)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := recordFromProto(message)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Version.Compare(record.Version) != consistency.Equal || decoded.Dependencies.Compare(record.Dependencies) != consistency.Equal || decoded.Origin != record.Origin || decoded.RaftIndex != record.RaftIndex {
		t.Fatalf("round trip = %#v, want %#v", decoded, record)
	}
}

func TestCausalReplicationPushOverGRPC(t *testing.T) {
	target, err := causal.NewReplica("target")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	registerCausalReplicationService(grpcServer, target)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	dialCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	connection, err := grpc.DialContext(dialCtx, listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	origin, err := causal.NewReplica("origin")
	if err != nil {
		t.Fatal(err)
	}
	record, err := origin.Write([]byte("/causal/k"), []byte("value"), false, nil, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	message, err := recordToProto(record)
	if err != nil {
		t.Fatal(err)
	}
	response, err := pb.NewCausalReplicationServiceClient(connection).Push(context.Background(), &pb.PushRequest{Record: message})
	if err != nil || !response.GetApplied() {
		t.Fatalf("Push = (%#v, %v)", response, err)
	}
	got, err := target.Read([]byte("/causal/k"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Values) != 1 || string(got.Values[0].Value) != "value" {
		t.Fatalf("replicated values = %#v", got.Values)
	}
}
