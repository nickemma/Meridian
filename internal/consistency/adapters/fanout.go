package adapters

import (
	"context"
	"log"
	"time"

	"github.com/nickemma/meridian/internal/consistency/ports"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Peer is one replication destination.
type Peer struct {
	ID      string
	Address string
}

// CredentialsFor returns the transport credentials used to reach a peer.
type CredentialsFor func(peerID string) credentials.TransportCredentials

// Send performs one RPC over an established peer connection.
type Send func(ctx context.Context, connection *grpc.ClientConn) error

const peerRPCTimeout = 200 * time.Millisecond

// FanOut asynchronously sends to every peer without making the caller's
// latency depend on another replica. There is deliberately no durability or
// retry here: receivers are idempotent, and the eventual path adds periodic
// anti-entropy on top. verb names the RPC in logs ("push", "gossip").
type FanOut struct {
	class       string
	verb        string
	peers       []Peer
	credentials CredentialsFor
	observer    ports.ReplicationObserver
}

func NewFanOut(class, verb string, peers []Peer, credentials CredentialsFor, observer ports.ReplicationObserver) *FanOut {
	return &FanOut{class: class, verb: verb, peers: append([]Peer(nil), peers...), credentials: credentials, observer: observer}
}

func (f *FanOut) Send(send Send) {
	for _, peer := range f.peers {
		go f.sendTo(peer, send)
	}
}

func (f *FanOut) sendTo(peer Peer, send Send) {
	ctx, cancel := context.WithTimeout(context.Background(), peerRPCTimeout)
	defer cancel()
	connection, err := grpc.DialContext(ctx, peer.Address, grpc.WithTransportCredentials(f.credentials(peer.ID)), grpc.WithBlock())
	if err != nil {
		f.observe(peer.ID, err)
		log.Printf("[%s] connect to %s: %v", f.class, peer.ID, err)
		return
	}
	defer connection.Close()
	err = send(ctx, connection)
	f.observe(peer.ID, err)
	if err != nil {
		log.Printf("[%s] %s to %s: %v", f.class, f.verb, peer.ID, err)
	}
}

func (f *FanOut) observe(peer string, err error) {
	if f.observer != nil {
		f.observer.Replicated(f.class, peer, err)
	}
}
