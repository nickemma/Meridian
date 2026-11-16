// Package adapters binds Meridian's authorization rules to X.509 mutual TLS
// and gRPC: certificate loading, listener options, interceptors and dial
// credentials.
package adapters

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

	"github.com/nickemma/meridian/internal/security/application"
	"github.com/nickemma/meridian/internal/security/domain"
	"github.com/nickemma/meridian/internal/security/ports"
	causalpb "github.com/nickemma/meridian/proto/causal"
	kv "github.com/nickemma/meridian/proto/kv"
	raftpb "github.com/nickemma/meridian/proto/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Bundle is one node's loaded credentials plus its authorizer. A nil *Bundle
// is the isolated plaintext development mode; every method handles it.
type Bundle struct {
	cert  tls.Certificate
	roots *x509.CertPool
	authz *application.Authorizer
}

var _ ports.PeerCredentials = (*Bundle)(nil)

// Load returns nil, nil for the zero Config (development mode). Any partial or
// inconsistent configuration fails closed.
func Load(cfg domain.Config, nodeID string, peerIDs []string) (*Bundle, error) {
	if !cfg.Enabled() {
		return nil, nil
	}
	if cfg.ClientsFile == "" {
		return nil, fmt.Errorf("secure mode requires a client authorization file")
	}
	cert, roots, err := loadPair(cfg.CAFile, cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("node TLS: %w", err)
	}
	if err := verifyNodeCertificate(cert.Leaf, nodeID); err != nil {
		return nil, err
	}
	rules, err := readRules(cfg.ClientsFile)
	if err != nil {
		return nil, err
	}
	authz, err := application.NewAuthorizer(nodeID, peerIDs, rules)
	if err != nil {
		return nil, err
	}
	return &Bundle{cert: cert, roots: roots, authz: authz}, nil
}

// PeerCredentials dials a peer and accepts only that registered peer's
// certificate.
func (b *Bundle) PeerCredentials(id string) credentials.TransportCredentials {
	if b == nil {
		return insecure.NewCredentials()
	}
	return credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: b.roots, Certificates: []tls.Certificate{b.cert}, ServerName: id,
		VerifyConnection: func(s tls.ConnectionState) error {
			if !b.authz.IsRegisteredPeer(certificateIdentity(s.PeerCertificates), id) {
				return fmt.Errorf("unregistered destination peer")
			}
			return nil
		}})
}

// ClientCredentials authenticates a client certificate and verifies destination
// hostname plus node role. ServerName may override the network hostname for tunnels.
func ClientCredentials(ca, cert, key, serverName string) (credentials.TransportCredentials, error) {
	pair, roots, err := loadPair(ca, cert, key)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{pair}, ServerName: serverName,
		VerifyConnection: func(s tls.ConnectionState) error {
			if !domain.IsNodeIdentity(certificateIdentity(s.PeerCertificates)) {
				return fmt.Errorf("destination is not a Meridian node")
			}
			return nil
		}}), nil
}

// Options returns server options for the peer (peers=true) or client
// listener. Role separation happens during the handshake; per-request checks
// happen in the interceptor before any handler runs.
func (b *Bundle) Options(peers bool) []grpc.ServerOption {
	if b == nil {
		return nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{b.cert}, ClientCAs: b.roots, ClientAuth: tls.RequireAndVerifyClientCert, SessionTicketsDisabled: true,
		VerifyConnection: func(s tls.ConnectionState) error {
			id := certificateIdentity(s.PeerCertificates)
			if (peers && b.authz.IsPeer(id)) || (!peers && b.authz.IsClient(id)) {
				return nil
			}
			return fmt.Errorf("workload is not authorized on this listener")
		}}
	interceptor := b.authorizeClient
	if peers {
		interceptor = b.authorizePeer
	}
	return []grpc.ServerOption{grpc.Creds(credentials.NewTLS(cfg)), grpc.ChainUnaryInterceptor(interceptor)}
}

func authenticated(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return ""
	}
	return certificateIdentity(info.State.PeerCertificates)
}

func (b *Bundle) authorizePeer(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	id := authenticated(ctx)
	if !b.authz.IsPeer(id) {
		return nil, denialStatus(domain.ErrPeerNotAuthorized)
	}
	var sender string
	claimsSender := true
	switch r := req.(type) {
	case *raftpb.RequestVoteRequest:
		sender = r.GetCandidateId()
	case *raftpb.PreVoteRequest:
		sender = r.GetCandidateId()
	case *raftpb.AppendEntriesRequest:
		sender = r.GetLeaderId()
	case *causalpb.PushRequest, *causalpb.GossipRequest:
		claimsSender = false
	default:
		return nil, denialStatus(domain.ErrPeerOperationDenied)
	}
	if err := b.authz.AuthorizePeer(id, sender, claimsSender); err != nil {
		return nil, denialStatus(err)
	}
	return next(ctx, req)
}

func (b *Bundle) authorizeClient(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	id := authenticated(ctx)
	if !b.authz.IsClient(id) {
		return nil, denialStatus(domain.ErrClientIdentityRequired)
	}
	op, key, known := clientOperation(req)
	if !known {
		return nil, denialStatus(domain.ErrOperationDenied)
	}
	if err := b.authz.AuthorizeClient(id, op, key); err != nil {
		return nil, denialStatus(err)
	}
	return next(ctx, req)
}

// clientOperation maps a KV request to the permission it needs. Status is a
// metadata-only request (zero operation).
func clientOperation(req any) (domain.Operation, []byte, bool) {
	switch r := req.(type) {
	case *kv.StatusRequest:
		return 0, nil, true
	case *kv.GetRequest:
		return domain.OperationRead, r.GetKey(), true
	case *kv.PutRequest:
		return domain.OperationWrite, r.GetKey(), true
	case *kv.DeleteRequest:
		return domain.OperationWrite, r.GetKey(), true
	case *kv.CompareAndSetRequest:
		return domain.OperationReadWrite, r.GetKey(), true
	default:
		return 0, nil, false
	}
}

func denialStatus(err error) error {
	var denial *domain.Denial
	if errors.As(err, &denial) && denial.Kind == domain.Unauthenticated {
		return status.Error(codes.Unauthenticated, denial.Reason)
	}
	return status.Error(codes.PermissionDenied, err.Error())
}
