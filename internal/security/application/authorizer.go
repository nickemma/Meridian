// Package application decides whether an authenticated workload may perform a
// storage or replication operation. It sees identities, never certificates.
package application

import (
	"fmt"

	"github.com/nickemma/meridian/internal/security/domain"
)

// Authorizer is deny-by-default: only registered peers reach the replication
// listener and only registered clients reach the storage listener.
type Authorizer struct {
	peers map[string]bool
	rules map[string]domain.Rule
}

// NewAuthorizer validates the peer set and client rules for nodeID.
func NewAuthorizer(nodeID string, peerIDs []string, rules []domain.Rule) (*Authorizer, error) {
	a := &Authorizer{peers: map[string]bool{}, rules: map[string]domain.Rule{}}
	for _, id := range peerIDs {
		if !domain.ValidSlug(id) || id == nodeID || a.peers[domain.NodeIdentity(id)] {
			return nil, fmt.Errorf("invalid or duplicate peer ID")
		}
		a.peers[domain.NodeIdentity(id)] = true
	}
	for _, r := range rules {
		if !r.Valid() {
			return nil, fmt.Errorf("invalid client policy")
		}
		if _, ok := a.rules[domain.ClientIdentity(r.Identity)]; ok {
			return nil, fmt.Errorf("duplicate client identity")
		}
		a.rules[domain.ClientIdentity(r.Identity)] = r
	}
	if len(a.rules) == 0 {
		return nil, fmt.Errorf("secure mode needs at least one client policy")
	}
	return a, nil
}

// IsPeer reports whether identity is a registered peer node.
func (a *Authorizer) IsPeer(identity string) bool { return a.peers[identity] }

// IsClient reports whether identity has a client rule.
func (a *Authorizer) IsClient(identity string) bool {
	_, ok := a.rules[identity]
	return ok
}

// IsRegisteredPeer reports whether identity is exactly the registered peer id.
func (a *Authorizer) IsRegisteredPeer(identity, id string) bool {
	return identity == domain.NodeIdentity(id) && a.peers[identity]
}

// AuthorizeClient admits a storage operation on key. A zero op is a
// metadata-only request (status) that any registered client may make.
func (a *Authorizer) AuthorizeClient(identity string, op domain.Operation, key []byte) error {
	rule, ok := a.rules[identity]
	if !ok {
		return domain.ErrClientIdentityRequired
	}
	if op == 0 {
		return nil
	}
	if !rule.Permits(op) || !rule.OwnsKey(key) {
		return domain.ErrTenantDenied
	}
	return nil
}

// AuthorizePeer binds a Raft sender field to the authenticated certificate.
// sender is empty for peer RPCs that carry no sender claim.
func (a *Authorizer) AuthorizePeer(identity, sender string, claimsSender bool) error {
	if !a.peers[identity] {
		return domain.ErrPeerNotAuthorized
	}
	if claimsSender && identity != domain.NodeIdentity(sender) {
		return domain.ErrSenderMismatch
	}
	return nil
}
