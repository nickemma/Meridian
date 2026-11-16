package adapters

import (
	"net"
	"testing"
	"time"

	codec "github.com/nickemma/meridian/internal/consistency/adapters"
	"github.com/nickemma/meridian/internal/eventual/application"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGossipOverGRPCConverges(t *testing.T) {
	target, err := application.NewReplica("target")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	RegisterService(server, target)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	origin, err := application.NewReplica("origin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := origin.Write([]byte("/eventual/k"), []byte("v"), false, 1); err != nil {
		t.Fatal(err)
	}
	gossip := NewGossip([]codec.Peer{{ID: "target", Address: listener.Addr().String()}},
		func(string) credentials.TransportCredentials { return insecure.NewCredentials() }, nil)
	gossip(origin.Records())

	deadline := time.Now().Add(5 * time.Second)
	for {
		got := target.Read([]byte("/eventual/k"))
		if len(got.Values) == 1 && string(got.Values[0].Value) == "v" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("gossiped values = %#v", got.Values)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
