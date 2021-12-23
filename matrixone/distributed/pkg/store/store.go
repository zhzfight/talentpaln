package store

import (
	"github.com/matrixorigin/talent-challenge/matrixbase/distributed/pkg/cfg"
)

// Store the Store interface
type Store interface {
	// Set set key-value to Store
	Set(key []byte, value []byte) error
	// Get returns the value from Store
	Get(key []byte) ([]byte, error)
	// Delete remove the key from Store
	Delete(key []byte) error
}

// NewStore create the raft Store
func NewStore(cfg cfg.Cfg) (Store, error) {

	if cfg.Store.Memory {
		return newMemoryStore(cfg, false)
	}

	// TODO: need to implement
	return newMemoryStore(cfg, true)
}
