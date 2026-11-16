// Package adapters provides the secrets layer's infrastructure.
package adapters

import (
	"crypto/rand"
	"fmt"

	"github.com/nickemma/meridian/internal/secrets/ports"
)

// RandomID generates 128-bit identifiers from crypto/rand.
type RandomID struct{}

var _ ports.IDGenerator = RandomID{}

func (RandomID) NewID() string {
	b := make([]byte, 16) // 128 bits
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("failed to generate request ID: %v", err))
	}
	return fmt.Sprintf("%x", b)
}
