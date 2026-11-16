//go:build !storageffi

package adapters

import (
	"fmt"

	config "github.com/nickemma/meridian/internal/config/domain"
	raftports "github.com/nickemma/meridian/internal/raft/ports"
	storageports "github.com/nickemma/meridian/internal/storage/ports"
)

func newStateMachine(*config.Config) (raftports.StateMachine, storageports.KVStore, func() error, error) {
	return nil, nil, nil, fmt.Errorf("Meridian was built without the storageffi tag; rebuild with make build-storageffi")
}
