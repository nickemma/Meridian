package application

import (
	"time"

	"github.com/nickemma/meridian/internal/secrets/domain"
	"github.com/nickemma/meridian/internal/secrets/ports"
)

// Commands builds validated secret commands with a fresh idempotency key and
// the current UTC time.
type Commands struct {
	ids ports.IDGenerator
	now func() time.Time
}

func NewCommands(ids ports.IDGenerator) Commands {
	return Commands{ids: ids, now: func() time.Time { return time.Now().UTC() }}
}

func (c Commands) Put(path string, value []byte, createdBy string) (*domain.Command, error) {
	return domain.PutCommand(path, value, createdBy, c.ids.NewID(), c.now())
}

func (c Commands) Delete(path, createdBy string) (*domain.Command, error) {
	return domain.DeleteCommand(path, createdBy, c.ids.NewID(), c.now())
}

func (c Commands) Rotate(path string, newValue []byte, createdBy string, gracePeriod time.Duration) (*domain.Command, error) {
	return domain.RotateCommand(path, newValue, createdBy, gracePeriod, c.ids.NewID(), c.now())
}
