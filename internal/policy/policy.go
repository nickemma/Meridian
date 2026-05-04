package policy

import (
	"fmt"
	"net"
	"slices"
	"strings"
	"time"
)

// Action represents what a service wants to do with a secret.
type Action string

const (
	ActionRead   Action = "read"
	ActionWrite  Action = "write"
	ActionRotate Action = "rotate"
	ActionDelete Action = "delete"
	ActionList   Action = "list"
)

// Request is the full context passed to the policy engine
// for every secret access attempt.
type Request struct {
	Identity  string    // service's mTLS certificate identity e.g. "payments-service"
	Path      string    // secret path e.g. "services/payments/db-password"
	Action    Action    // what the service wants to do
	SourceIP  string    // where the request came from
	Timestamp time.Time // when the request arrived
}

// Decision is the policy engine's verdict on a request.
type Decision struct {
	Allowed bool
	Reason  string // human-readable explanation — logged on both allow and deny
	Policy  string // which policy matched
}

// Rule is a single condition within a policy.
// All rules in a policy must match for the policy to allow access.
type Rule struct {
	// PathPrefix — secret path must start with this prefix.
	// Empty means match any path.
	PathPrefix string `json:"path_prefix,omitempty"`

	// Identities — service identity must be in this list.
	// Empty means match any identity.
	Identities []string `json:"identities,omitempty"`

	// Actions — requested action must be in this list.
	// Empty means match any action.
	Actions []Action `json:"actions,omitempty"`

	// AllowedCIDRs — source IP must be within one of these ranges.
	// Empty means match any IP.
	AllowedCIDRs []string `json:"allowed_cidrs,omitempty"`

	// BusinessHoursOnly — if true, only allow between 06:00 and 22:00 UTC.
	BusinessHoursOnly bool `json:"business_hours_only,omitempty"`
}

// Policy is a named set of rules that grants access to secrets.
// A request is allowed if ALL rules in the policy match.
// Deny-by-default — if no policy matches, access is denied.
type Policy struct {
	Name    string `json:"name"`
	Rules   []Rule `json:"rules"`
	Version uint64 `json:"version"`
}

// Matches returns true if all rules in the policy match the request.
func (p *Policy) Matches(req *Request) (bool, string) {
	for _, rule := range p.Rules {
		if ok, reason := rule.matches(req); !ok {
			return false, reason
		}
	}
	return true, ""
}

func (r *Rule) matches(req *Request) (bool, string) {
	// Check path prefix
	if r.PathPrefix != "" && !strings.HasPrefix(req.Path, r.PathPrefix) {
		return false, fmt.Sprintf("path %q does not match prefix %q",
			req.Path, r.PathPrefix)
	}

	// Check identity
	if len(r.Identities) > 0 && !contains(r.Identities, req.Identity) {
		return false, fmt.Sprintf("identity %q not in allowed list", req.Identity)
	}

	// Check action
	if len(r.Actions) > 0 && !containsAction(r.Actions, req.Action) {
		return false, fmt.Sprintf("action %q not in allowed list", req.Action)
	}

	// Check CIDR
	if len(r.AllowedCIDRs) > 0 {
		if !ipInCIDRs(req.SourceIP, r.AllowedCIDRs) {
			return false, fmt.Sprintf("source IP %q not in allowed CIDRs", req.SourceIP)
		}
	}

	// Check business hours
	if r.BusinessHoursOnly {
		hour := req.Timestamp.UTC().Hour()
		if hour < 6 || hour >= 22 {
			return false, fmt.Sprintf("access denied outside business hours (hour=%d UTC)", hour)
		}
	}

	return true, ""
}

func contains(list []string, val string) bool {
	return slices.Contains(list, val)
}

func containsAction(list []Action, val Action) bool {
	return slices.Contains(list, val)
}

func ipInCIDRs(ipStr string, cidrs []string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}
	for _, cidr := range cidrs {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			continue
		}
		if network.Contains(ip) {
			return true
		}
	}
	return false
}
