//go:build !storageffi

package server

import (
	"fmt"

	"github.com/nickemma/meridian/internal/config"
	"github.com/nickemma/meridian/internal/raft"
)

func newStateMachine(*config.Config) (raft.StateMachine, raft.KVStore, func() error, error) {
	return nil, nil, nil, fmt.Errorf("Meridian was built without the storageffi tag; rebuild with make build-storageffi")
}
