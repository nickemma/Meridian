package secrets

import (
	"fmt"
	"sync"
	"time"
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
	secrets  map[string]*Secret         // path → current secret
	versions map[string][]SecretVersion // path → all versions
}

// NewStore creates an empty secrets store.
func NewStore() *Store {
	return &Store{
		secrets:  make(map[string]*Secret),
		versions: make(map[string][]SecretVersion),
	}
}

// Apply processes a committed Raft command and updates the store.
// Called by the Raft apply loop — never called directly by clients.
// Must be deterministic — same command always produces same state.
func (s *Store) Apply(cmd *Command) error {
	switch cmd.Type {
	case CmdPutSecret:
		return s.applyPut(cmd)
	case CmdDeleteSecret:
		return s.applyDelete(cmd)
	case CmdRotateSecret:
		return s.applyRotate(cmd)
	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

func (s *Store) applyPut(cmd *Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.secrets[cmd.Path]

	version := uint64(1)
	if exists {
		version = existing.Version + 1
	}

	secret := &Secret{
		Path:      cmd.Path,
		Value:     cmd.Value,
		Version:   version,
		CreatedAt: cmd.Timestamp,
		UpdatedAt: cmd.Timestamp,
		CreatedBy: cmd.CreatedBy,
	}

	s.secrets[cmd.Path] = secret

	// Keep version history
	s.versions[cmd.Path] = append(s.versions[cmd.Path], SecretVersion{
		Version:   version,
		Value:     cmd.Value,
		CreatedAt: cmd.Timestamp,
		CreatedBy: cmd.CreatedBy,
	})

	return nil
}

func (s *Store) applyDelete(cmd *Command) error {
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

func (s *Store) applyRotate(cmd *Command) error {
	// Rotation is a put with a new value — the old version
	// stays in history with a grace period before revocation.
	return s.applyPut(cmd)
}

// --- Read operations ---
// These are safe to call concurrently from any goroutine.

// Get returns the current version of a secret.
// Returns ErrNotFound if the path does not exist.
func (s *Store) Get(path string) (*Secret, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	secret, exists := s.secrets[path]
	if !exists {
		return nil, ErrNotFound{Path: path}
	}

	// Return a copy — callers must not mutate the store's state.
	copy := *secret
	return &copy, nil
}

// GetVersion returns a specific historical version of a secret.
func (s *Store) GetVersion(path string, version uint64) (*SecretVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	versions, exists := s.versions[path]
	if !exists {
		return nil, ErrNotFound{Path: path}
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
func (s *Store) ListVersions(path string) ([]SecretVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	versions, exists := s.versions[path]
	if !exists {
		return nil, ErrNotFound{Path: path}
	}

	// Return a copy of the slice
	result := make([]SecretVersion, len(versions))
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

// ErrNotFound is returned when a secret path does not exist.
type ErrNotFound struct {
	Path string
}

func (e ErrNotFound) Error() string {
	return fmt.Sprintf("secret not found: %s", e.Path)
}

// --- Validation ---

// ValidatePath checks that a secret path is well-formed.
// Paths must be slash-separated, non-empty segments.
// e.g. "services/payments/db-password" is valid
//
//	"/leading-slash" is not
func ValidatePath(path string) error {
	if path == "" {
		return fmt.Errorf("secret path cannot be empty")
	}
	if path[0] == '/' {
		return fmt.Errorf("secret path must not start with /")
	}
	if path[len(path)-1] == '/' {
		return fmt.Errorf("secret path must not end with /")
	}
	return nil
}

// ValidateValue checks that a secret value is within size limits.
func ValidateValue(value []byte) error {
	const maxSize = 64 * 1024 // 64KB
	if len(value) == 0 {
		return fmt.Errorf("secret value cannot be empty")
	}
	if len(value) > maxSize {
		return fmt.Errorf("secret value exceeds maximum size of 64KB")
	}
	return nil
}

// --- Command builders ---
// These are the only way to create commands — enforces validation.

// NewPutCommand builds a validated put command.
func NewPutCommand(path string, value []byte, createdBy string) (*Command, error) {
	if err := ValidatePath(path); err != nil {
		return nil, err
	}
	if err := ValidateValue(value); err != nil {
		return nil, err
	}
	return &Command{
		Type:      CmdPutSecret,
		Path:      path,
		Value:     value,
		CreatedBy: createdBy,
		RequestID: newRequestID(),
		Timestamp: time.Now().UTC(),
	}, nil
}

// NewDeleteCommand builds a validated delete command.
func NewDeleteCommand(path string, createdBy string) (*Command, error) {
	if err := ValidatePath(path); err != nil {
		return nil, err
	}
	return &Command{
		Type:      CmdDeleteSecret,
		Path:      path,
		CreatedBy: createdBy,
		RequestID: newRequestID(),
		Timestamp: time.Now().UTC(),
	}, nil
}

// NewRotateCommand builds a validated rotate command.
func NewRotateCommand(path string, newValue []byte, createdBy string) (*Command, error) {
	if err := ValidatePath(path); err != nil {
		return nil, err
	}
	if err := ValidateValue(newValue); err != nil {
		return nil, err
	}
	return &Command{
		Type:      CmdRotateSecret,
		Path:      path,
		Value:     newValue,
		CreatedBy: createdBy,
		RequestID: newRequestID(),
		Timestamp: time.Now().UTC(),
	}, nil
}
