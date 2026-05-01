package raft

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/nickemma/meridian/proto/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PeerClient manages the gRPC connection to a single peer node.
// Connections are lazy — we don't connect until the first RPC call.
// If a connection fails, we retry on the next call.
type PeerClient struct {
	mu      sync.Mutex
	id      string // peer's node ID e.g. "node-2"
	address string // peer's address e.g. "node-2:9090"
	conn    *grpc.ClientConn
	client  pb.RaftServiceClient
}

// NewPeerClient creates a client for the given peer.
// Does not connect immediately — connection is established on first use.
func NewPeerClient(id, address string) *PeerClient {
	return &PeerClient{
		id:      id,
		address: address,
	}
}

// connect establishes the gRPC connection if not already connected.
// Caller must hold p.mu.
func (p *PeerClient) connect() error {
	if p.conn != nil {
		return nil
	}

	conn, err := grpc.NewClient(
		p.address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("connecting to peer %s at %s: %w", p.id, p.address, err)
	}

	p.conn = conn
	p.client = pb.NewRaftServiceClient(conn)
	return nil
}

// RequestVote sends a RequestVote RPC to this peer.
// Returns nil response and error if the peer is unreachable.
func (p *PeerClient) RequestVote(
	ctx context.Context,
	req *pb.RequestVoteRequest,
) (*pb.RequestVoteResponse, error) {
	p.mu.Lock()
	if err := p.connect(); err != nil {
		p.mu.Unlock()
		return nil, err
	}
	client := p.client
	p.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	return client.RequestVote(ctx, req)
}

// AppendEntries sends an AppendEntries RPC to this peer.
func (p *PeerClient) AppendEntries(
	ctx context.Context,
	req *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {
	p.mu.Lock()
	if err := p.connect(); err != nil {
		p.mu.Unlock()
		return nil, err
	}
	client := p.client
	p.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	return client.AppendEntries(ctx, req)
}

// Close tears down the gRPC connection.
func (p *PeerClient) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			log.Printf("[peer] error closing connection to %s: %v", p.id, err)
		}
		p.conn = nil
		p.client = nil
	}
}

// ID returns the peer's node ID.
func (p *PeerClient) ID() string {
	return p.id
}
