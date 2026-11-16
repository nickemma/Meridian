// Package adapters reads node configuration from the process environment.
package adapters

import (
	"os"

	"github.com/nickemma/meridian/internal/config/application"
	"github.com/nickemma/meridian/internal/config/domain"
	"github.com/nickemma/meridian/internal/config/ports"
)

// Environment is a Source backed by os.LookupEnv.
type Environment struct{}

var _ ports.Source = Environment{}

func (Environment) Lookup(key string) (string, bool) { return os.LookupEnv(key) }

// LoadFromEnvironment builds the node Config from MERIDIAN_* variables.
func LoadFromEnvironment() (*domain.Config, error) {
	return application.Load(Environment{})
}
