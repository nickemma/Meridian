package application

import (
	"errors"
	"testing"

	"github.com/nickemma/meridian/internal/security/domain"
)

func TestAuthorizerIsDenyByDefault(t *testing.T) {
	a, err := NewAuthorizer("node-1", []string{"node-2"}, []domain.Rule{{Identity: "tenant-a", Tenant: "a", Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.AuthorizeClient(domain.ClientIdentity("tenant-a"), domain.OperationRead, []byte("/tenants/a/x")); err != nil {
		t.Fatalf("authorized read denied: %v", err)
	}
	if err := a.AuthorizeClient(domain.ClientIdentity("tenant-a"), domain.OperationWrite, []byte("/tenants/a/x")); !errors.Is(err, domain.ErrTenantDenied) {
		t.Fatalf("read-only write = %v", err)
	}
	if err := a.AuthorizeClient(domain.ClientIdentity("other"), 0, nil); !errors.Is(err, domain.ErrClientIdentityRequired) {
		t.Fatalf("unknown client = %v", err)
	}
	if err := a.AuthorizePeer(domain.NodeIdentity("node-2"), "node-3", true); !errors.Is(err, domain.ErrSenderMismatch) {
		t.Fatalf("forged sender = %v", err)
	}
	if err := a.AuthorizePeer(domain.NodeIdentity("node-3"), "", false); !errors.Is(err, domain.ErrPeerNotAuthorized) {
		t.Fatalf("unknown peer = %v", err)
	}
}

func TestAuthorizerRejectsInvalidConfiguration(t *testing.T) {
	for _, test := range []struct {
		peers []string
		rules []domain.Rule
	}{
		{nil, nil},
		{[]string{"node-1"}, []domain.Rule{{Identity: "a", Tenant: "a", Read: true}}},
		{[]string{"node-2", "node-2"}, []domain.Rule{{Identity: "a", Tenant: "a", Read: true}}},
		{nil, []domain.Rule{{Identity: "a", Tenant: "../b", Read: true}}},
		{nil, []domain.Rule{{Identity: "a", Tenant: "a"}}},
		{nil, []domain.Rule{{Identity: "a", Tenant: "a", Read: true}, {Identity: "a", Tenant: "b", Read: true}}},
	} {
		if _, err := NewAuthorizer("node-1", test.peers, test.rules); err == nil {
			t.Fatalf("invalid configuration accepted: %+v", test)
		}
	}
}
