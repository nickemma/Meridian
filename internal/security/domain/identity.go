// Package domain holds Meridian's workload identities and the storage
// authorization rules, independent of X.509 and gRPC.
package domain

import (
	"path"
	"regexp"
	"strings"
)

// Config names the opt-in mutual-TLS material. The zero value selects the
// isolated plaintext development mode.
type Config struct{ CAFile, CertFile, KeyFile, ClientsFile string }

// Enabled reports whether any security material was configured.
func (c Config) Enabled() bool { return c != (Config{}) }

const (
	NodePrefix   = "urn:meridian:node:"
	ClientPrefix = "urn:meridian:client:"
)

// NodeIdentity and ClientIdentity return the certificate URI SAN for an ID.
func NodeIdentity(id string) string   { return NodePrefix + id }
func ClientIdentity(id string) string { return ClientPrefix + id }

// IsNodeIdentity reports whether identity names any Meridian node.
func IsNodeIdentity(identity string) bool { return strings.HasPrefix(identity, NodePrefix) }

var slug = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

// ValidSlug reports whether id is a safe node, client or tenant identifier.
func ValidSlug(id string) bool { return slug.MatchString(id) }

// Rule grants one client identity read and/or write access to one tenant.
type Rule struct {
	Identity string `json:"identity"`
	Tenant   string `json:"tenant"`
	Read     bool   `json:"read"`
	Write    bool   `json:"write"`
}

// Valid reports whether the rule names safe identifiers and grants something.
func (r Rule) Valid() bool {
	return ValidSlug(r.Identity) && ValidSlug(r.Tenant) && (r.Read || r.Write)
}

// Operation is the permission a storage request needs.
type Operation uint8

const (
	OperationRead Operation = iota + 1
	OperationWrite
	// OperationReadWrite is compare-and-set: it observes and mutates.
	OperationReadWrite
)

// Permits reports whether the rule's flags allow op.
func (r Rule) Permits(op Operation) bool {
	switch op {
	case OperationRead:
		return r.Read
	case OperationWrite:
		return r.Write
	case OperationReadWrite:
		return r.Read && r.Write
	default:
		return false
	}
}

// OwnsKey reports whether key lies inside the rule's tenant namespace. Keys
// are opaque storage bytes, not URL-decoded or filesystem paths, so exactly one
// canonical absolute form is accepted and namespace ownership has a single
// meaning.
func (r Rule) OwnsKey(key []byte) bool {
	text := string(key)
	return strings.HasPrefix(text, TenantPrefix(r.Tenant)) && path.Clean(text) == text && !strings.ContainsAny(text, "\x00\\")
}

// TenantPrefix is the namespace every key of tenant must start with.
func TenantPrefix(tenant string) string { return "/tenants/" + tenant + "/" }
