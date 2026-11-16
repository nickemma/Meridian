package domain

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Class is the consistency mechanism a request or namespace uses.
type Class uint8

const (
	Strong Class = iota + 1
	Causal
	Eventual
)

func (c Class) String() string {
	switch c {
	case Strong:
		return "strong"
	case Causal:
		return "causal"
	case Eventual:
		return "eventual"
	default:
		return "unknown"
	}
}

// ParseClass accepts the lower-case names produced by Class.String.
func ParseClass(name string) (Class, error) {
	switch name {
	case "strong":
		return Strong, nil
	case "causal":
		return Causal, nil
	case "eventual":
		return Eventual, nil
	default:
		return 0, fmt.Errorf("invalid policy class %q", name)
	}
}

// Strength orders classes so a read may be weaker than, but never stronger
// than, its namespace. Unknown classes have strength zero.
func (c Class) Strength() uint8 {
	switch c {
	case Strong:
		return 3
	case Causal:
		return 2
	case Eventual:
		return 1
	default:
		return 0
	}
}

// NamespacePolicy assigns exactly one write path to a path prefix. Policies
// are immutable after installation; production installation will be a strong
// Raft command so every node uses the same longest-prefix result.
type NamespacePolicy struct {
	Prefix  string
	Class   Class
	Version uint64
}

var (
	ErrNoPolicy      = errors.New("no namespace policy matches key")
	ErrPolicyExists  = errors.New("namespace policy is immutable")
	ErrWriteClass    = errors.New("write consistency does not match namespace policy")
	ErrReadTooStrong = errors.New("requested read consistency exceeds namespace policy")
)

// DefaultPolicies is the explicit development-only root strong policy used
// when no namespace policy is configured.
func DefaultPolicies() []NamespacePolicy {
	return []NamespacePolicy{{Prefix: "/", Class: Strong, Version: 1}}
}

// ValidatePrefix accepts "/" or an absolute path ending in "/".
func ValidatePrefix(prefix string) error {
	if prefix == "/" {
		return nil
	}
	if !strings.HasPrefix(prefix, "/") || !strings.HasSuffix(prefix, "/") {
		return fmt.Errorf("namespace prefix must be / or an absolute path ending in /")
	}
	return nil
}

// Validate rejects a policy that could never be installed.
func (p NamespacePolicy) Validate() error {
	if err := ValidatePrefix(p.Prefix); err != nil {
		return err
	}
	if p.Version == 0 {
		return fmt.Errorf("namespace policy version is required")
	}
	if p.Class.Strength() == 0 {
		return fmt.Errorf("namespace policy has invalid consistency class")
	}
	return nil
}

// AdmitWrite checks a write against the policy selected for key.
func (p NamespacePolicy) AdmitWrite(key string, requested Class, expectedVersion uint64) error {
	if p.Version != expectedVersion {
		return fmt.Errorf("policy version %d does not match current version %d", expectedVersion, p.Version)
	}
	if requested != p.Class {
		return fmt.Errorf("%w: key %s is %s", ErrWriteClass, key, p.Class)
	}
	return nil
}

// AdmitRead checks that requested is no stronger than the namespace class.
func (p NamespacePolicy) AdmitRead(key string, requested Class) error {
	if requested.Strength() > p.Class.Strength() {
		return fmt.Errorf("%w: key %s is %s", ErrReadTooStrong, key, p.Class)
	}
	return nil
}

// ParsePolicies accepts comma-separated prefix:class:version entries, for
// example "/strong/:strong:1,/causal/:causal:2,/eventual/:eventual:3". Empty
// input returns no policies so the caller can choose DefaultPolicies.
func ParsePolicies(raw string) ([]NamespacePolicy, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	entries := strings.Split(raw, ",")
	policies := make([]NamespacePolicy, 0, len(entries))
	for _, entry := range entries {
		parts := strings.Split(strings.TrimSpace(entry), ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid policy %q; expected prefix:class:version", entry)
		}
		version, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil || version == 0 {
			return nil, fmt.Errorf("invalid policy version %q", parts[2])
		}
		class, err := ParseClass(parts[1])
		if err != nil {
			return nil, err
		}
		if ValidatePrefix(parts[0]) != nil {
			return nil, fmt.Errorf("invalid policy prefix %q", parts[0])
		}
		policies = append(policies, NamespacePolicy{Prefix: parts[0], Class: class, Version: version})
	}
	return policies, nil
}
