package secrets

import (
	"testing"
	"time"
)

func makeCmd(t CommandType, path string, value []byte) *Command {
	return &Command{
		Type:      t,
		Path:      path,
		Value:     value,
		CreatedBy: "test-service",
		RequestID: "test-req-id",
		Timestamp: time.Now().UTC(),
	}
}

func TestStore_PutAndGet(t *testing.T) {
	s := NewStore()

	err := s.Apply(makeCmd(CmdPutSecret, "services/db/password", []byte("secret")))
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}

	secret, err := s.Get("services/db/password")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	if string(secret.Value) != "secret" {
		t.Errorf("expected 'secret', got %s", secret.Value)
	}
	if secret.Version != 1 {
		t.Errorf("expected version 1, got %d", secret.Version)
	}
}

func TestStore_VersionIncrementsOnUpdate(t *testing.T) {
	s := NewStore()

	s.Apply(makeCmd(CmdPutSecret, "svc/key", []byte("v1")))
	s.Apply(makeCmd(CmdPutSecret, "svc/key", []byte("v2")))

	secret, _ := s.Get("svc/key")
	if secret.Version != 2 {
		t.Errorf("expected version 2, got %d", secret.Version)
	}
	if string(secret.Value) != "v2" {
		t.Errorf("expected 'v2', got %s", secret.Value)
	}
}

func TestStore_VersionHistoryPreserved(t *testing.T) {
	s := NewStore()

	s.Apply(makeCmd(CmdPutSecret, "svc/key", []byte("v1")))
	s.Apply(makeCmd(CmdPutSecret, "svc/key", []byte("v2")))
	s.Apply(makeCmd(CmdPutSecret, "svc/key", []byte("v3")))

	versions, err := s.ListVersions("svc/key")
	if err != nil {
		t.Fatalf("list versions failed: %v", err)
	}
	if len(versions) != 3 {
		t.Errorf("expected 3 versions, got %d", len(versions))
	}
	if string(versions[0].Value) != "v1" {
		t.Errorf("expected first version 'v1', got %s", versions[0].Value)
	}
}

func TestStore_DeleteRemovesSecret(t *testing.T) {
	s := NewStore()

	s.Apply(makeCmd(CmdPutSecret, "svc/key", []byte("value")))
	s.Apply(makeCmd(CmdDeleteSecret, "svc/key", nil))

	_, err := s.Get("svc/key")
	if err == nil {
		t.Error("expected error after delete, got nil")
	}
}

func TestStore_DeleteIsIdempotent(t *testing.T) {
	s := NewStore()

	// Deleting a non-existent path must not error
	err := s.Apply(makeCmd(CmdDeleteSecret, "svc/missing", nil))
	if err != nil {
		t.Errorf("expected idempotent delete, got error: %v", err)
	}
}

func TestStore_RotateIncrementsVersion(t *testing.T) {
	s := NewStore()

	s.Apply(makeCmd(CmdPutSecret, "svc/key", []byte("original")))
	s.Apply(makeCmd(CmdRotateSecret, "svc/key", []byte("rotated")))

	secret, _ := s.Get("svc/key")
	if secret.Version != 2 {
		t.Errorf("expected version 2 after rotation, got %d", secret.Version)
	}
	if string(secret.Value) != "rotated" {
		t.Errorf("expected 'rotated', got %s", secret.Value)
	}
}

func TestStore_GetNotFound(t *testing.T) {
	s := NewStore()

	_, err := s.Get("does/not/exist")
	if err == nil {
		t.Error("expected error for missing path")
	}
}

func TestValidatePath_RejectsEmpty(t *testing.T) {
	if err := ValidatePath(""); err == nil {
		t.Error("expected error for empty path")
	}
}

func TestValidatePath_RejectsLeadingSlash(t *testing.T) {
	if err := ValidatePath("/leading"); err == nil {
		t.Error("expected error for leading slash")
	}
}

func TestValidatePath_AcceptsValidPath(t *testing.T) {
	if err := ValidatePath("services/payments/db-password"); err != nil {
		t.Errorf("expected valid path to pass, got: %v", err)
	}
}

func TestCommandEncodeDecode_RoundTrip(t *testing.T) {
	cmd, _ := NewPutCommand("svc/key", []byte("value"), "test-service")

	encoded, err := cmd.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := DecodeCommand(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Path != cmd.Path {
		t.Errorf("path mismatch: got %s want %s", decoded.Path, cmd.Path)
	}
	if string(decoded.Value) != string(cmd.Value) {
		t.Errorf("value mismatch: got %s want %s", decoded.Value, cmd.Value)
	}
	if decoded.Type != cmd.Type {
		t.Errorf("type mismatch: got %d want %d", decoded.Type, cmd.Type)
	}
}
