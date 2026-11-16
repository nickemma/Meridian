package adapters

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nickemma/meridian/internal/raft/domain"
	"github.com/nickemma/meridian/internal/raft/ports"
	pb "github.com/nickemma/meridian/proto/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const peerRPCTimeout = 100 * time.Millisecond

// PeerClient manages the gRPC connection to a single peer node.
// Connections are lazy — we don't connect until the first RPC call.
// If a connection fails, we retry on the next call.
type PeerClient struct {
	transport credentials.TransportCredentials
	mu        sync.Mutex
	id        string // peer's node ID e.g. "node-2"
	address   string // peer's address e.g. "node-2:9090"
	conn      *grpc.ClientConn
	client    pb.RaftServiceClient
}

var _ ports.Peer = (*PeerClient)(nil)

// NewPeerClient creates a client for the given peer. A nil transport selects
// plaintext development credentials.
func NewPeerClient(id, address string, transport credentials.TransportCredentials) *PeerClient {
	if transport == nil {
		transport = insecure.NewCredentials()
	}
	return &PeerClient{id: id, address: address, transport: transport}
}

// ID returns the peer's node ID.
func (p *PeerClient) ID() string { return p.id }

// rpcClient returns the connected stub, establishing the connection if needed.
func (p *PeerClient) rpcClient() (pb.RaftServiceClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil {
		return p.client, nil
	}
	conn, err := grpc.NewClient(p.address, grpc.WithTransportCredentials(p.transport))
	if err != nil {
		return nil, fmt.Errorf("connecting to peer %s at %s: %w", p.id, p.address, err)
	}
	p.conn = conn
	p.client = pb.NewRaftServiceClient(conn)
	return p.client, nil
}

// call runs one bounded RPC against the peer.
func call[Req, Resp any](ctx context.Context, p *PeerClient, request Req, rpc func(pb.RaftServiceClient, context.Context, Req, ...grpc.CallOption) (Resp, error)) (Resp, error) {
	client, err := p.rpcClient()
	if err != nil {
		var zero Resp
		return zero, err
	}
	ctx, cancel := context.WithTimeout(ctx, peerRPCTimeout)
	defer cancel()
	return rpc(client, ctx, request)
}

func (p *PeerClient) RequestVote(ctx context.Context, req domain.VoteRequest) (domain.VoteResponse, error) {
	resp, err := call(ctx, p, voteRequestToProto(req), pb.RaftServiceClient.RequestVote)
	if err != nil {
		return domain.VoteResponse{}, err
	}
	return domain.VoteResponse{Term: resp.GetTerm(), VoteGranted: resp.GetVoteGranted()}, nil
}

func (p *PeerClient) PreVote(ctx context.Context, req domain.PreVoteRequest) (domain.PreVoteResponse, error) {
	resp, err := call(ctx, p, preVoteRequestToProto(req), pb.RaftServiceClient.PreVote)
	if err != nil {
		return domain.PreVoteResponse{}, err
	}
	return domain.PreVoteResponse{Term: resp.GetTerm(), VoteGranted: resp.GetVoteGranted()}, nil
}

func (p *PeerClient) AppendEntries(ctx context.Context, req domain.AppendEntriesRequest) (domain.AppendEntriesResponse, error) {
	resp, err := call(ctx, p, appendEntriesRequestToProto(req), pb.RaftServiceClient.AppendEntries)
	if err != nil {
		return domain.AppendEntriesResponse{}, err
	}
	return domain.AppendEntriesResponse{Term: resp.GetTerm(), Success: resp.GetSuccess(), MatchIndex: resp.GetMatchIndex()}, nil
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
