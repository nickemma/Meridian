package adapters

import (
	"path/filepath"

	"github.com/nickemma/meridian/internal/raft/application"
	"github.com/nickemma/meridian/internal/raft/domain"
	"github.com/nickemma/meridian/internal/raft/ports"
	"google.golang.org/grpc/credentials"
)

// PeerAddress is one peer's ID and Raft RPC address.
type PeerAddress struct {
	ID      string
	Address string
}

// CredentialsFor returns the transport credentials used to dial a peer.
type CredentialsFor func(peerID string) credentials.TransportCredentials

// OpenDurableNode restores Raft state from dataDir/raft before any peer
// connection is opened. A state-store error is fatal to node startup because
// participating with a fresh term or truncated log is unsafe.
func OpenDurableNode(opts domain.Options, dataDir string, peers []PeerAddress, credentialsFor CredentialsFor, stateMachine ports.StateMachine) (*application.Node, error) {
	store, err := NewFileStateStore(filepath.Join(dataDir, "raft"))
	if err != nil {
		return nil, err
	}
	state, err := application.NewStateFromStore(opts.NodeID, store)
	if err != nil {
		return nil, err
	}
	transports := make([]ports.Peer, len(peers))
	for i, p := range peers {
		var transport credentials.TransportCredentials
		if credentialsFor != nil {
			transport = credentialsFor(p.ID)
		}
		transports[i] = NewPeerClient(p.ID, p.Address, transport)
	}
	return application.NewNode(opts, state, transports, stateMachine), nil
}
