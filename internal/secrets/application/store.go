// Package application is the secrets layer: the deterministic secrets state
// machine, validated command construction and lease management.
package application

import (
	"fmt"
	"sync"

	"github.com/nickemma/meridian/internal/secrets/domain"
)

// Store is the in-memory secrets state machine.
// It is rebuilt by replaying the Raft log on every startup.
// All mutations come through Apply() — never directly.
//
// The store is the secrets layer's view of committed state.
// It does not talk to Raft directly — the Node does that.
// The store only knows how to apply committed commands.
type Store struct {
	mu       sync.RWMutex
	secrets  map[string]*domain.Secret         // path → current secret
	versions map[string][]domain.SecretVersion // path → all versions
}

// NewStore creates an empty secrets store.
func NewStore() *Store {
	return &Store{
		secrets:  make(map[string]*domain.Secret),
		versions: make(map[string][]domain.SecretVersion),
	}
}

// Apply processes a committed Raft command and updates the store.
// Called by the Raft apply loop — never called directly by clients.
// Must be deterministic — same command always produces same state.
func (s *Store) Apply(cmd *domain.Command) error {
	switch cmd.Type {
	case domain.CmdPutSecret:
		return s.applyPut(cmd)
	case domain.CmdDeleteSecret:
		return s.applyDelete(cmd)
	case domain.CmdRotateSecret:
		return s.applyRotate(cmd)
	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

func (s *Store) applyPut(cmd *domain.Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.secrets[cmd.Path]

	version := uint64(1)
	if exists {
		version = existing.Version + 1
	}

	secret := &domain.Secret{
		Path:      cmd.Path,
		Value:     cmd.Value,
		Version:   version,
		CreatedAt: cmd.Timestamp,
		UpdatedAt: cmd.Timestamp,
		CreatedBy: cmd.CreatedBy,
	}

	s.secrets[cmd.Path] = secret

	// Keep version history
	s.versions[cmd.Path] = append(s.versions[cmd.Path], domain.SecretVersion{
		Version:   version,
		Value:     cmd.Value,
		CreatedAt: cmd.Timestamp,
		CreatedBy: cmd.CreatedBy,
	})

	return nil
}

func (s *Store) applyDelete(cmd *domain.Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.secrets[cmd.Path]; !exists {
		// Idempotent — deleting a non-existent secret is fine.
		return nil
	}

	delete(s.secrets, cmd.Path)
	// Versions are kept for audit purposes even after deletion.
	return nil
}

func (s *Store) applyRotate(cmd *domain.Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.secrets[cmd.Path]

	version := uint64(1)

	if exists {
		version = existing.Version + 1

		// Schedule the old version for revocation after the grace period.
		// This allows services using the old credential time to reconnect.
		if cmd.GracePeriod > 0 && len(s.versions[cmd.Path]) > 0 {
			oldIdx := len(s.versions[cmd.Path]) - 1
			s.versions[cmd.Path][oldIdx].RevokeAt =
				cmd.Timestamp.Add(cmd.GracePeriod)
		}
	}

	secret := &domain.Secret{
		Path:      cmd.Path,
		Value:     cmd.Value,
		Version:   version,
		CreatedAt: cmd.Timestamp,
		UpdatedAt: cmd.Timestamp,
		CreatedBy: cmd.CreatedBy,
	}

	s.secrets[cmd.Path] = secret

	s.versions[cmd.Path] = append(s.versions[cmd.Path], domain.SecretVersion{
		Version:   version,
		Value:     cmd.Value,
		CreatedAt: cmd.Timestamp,
		CreatedBy: cmd.CreatedBy,
	})
	// stays in history with a grace period before revocation.
	return nil
}

// --- Read operations ---
// These are safe to call concurrently from any goroutine.

// Get returns the current version of a secret.
// Returns ErrNotFound if the path does not exist.
func (s *Store) Get(path string) (*domain.Secret, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	secret, exists := s.secrets[path]
	if !exists {
		return nil, domain.ErrNotFound{Path: path}
	}

	// Return a copy — callers must not mutate the store's state.
	copy := *secret
	return &copy, nil
}

// GetVersion returns a specific historical version of a secret.
func (s *Store) GetVersion(path string, version uint64) (*domain.SecretVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	versions, exists := s.versions[path]
	if !exists {
		return nil, domain.ErrNotFound{Path: path}
	}

	for _, v := range versions {
		if v.Version == version {
			copy := v
			return &copy, nil
		}
	}

	return nil, fmt.Errorf("version %d not found for path %s", version, path)
}

// ListVersions returns all historical versions for a path.
func (s *Store) ListVersions(path string) ([]domain.SecretVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	versions, exists := s.versions[path]
	if !exists {
		return nil, domain.ErrNotFound{Path: path}
	}

	// Return a copy of the slice
	result := make([]domain.SecretVersion, len(versions))
	copy(result, versions)
	return result, nil
}

// List returns all current secret paths.
func (s *Store) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	paths := make([]string, 0, len(s.secrets))
	for path := range s.secrets {
		paths = append(paths, path)
	}
	return paths
}
