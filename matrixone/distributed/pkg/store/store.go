package store

import (
	"github.com/matrixorigin/talent-challenge/matrixbase/distributed/pkg/cfg"
)

// Store the store interface
type Store interface {
	// Set set key-value to store
	Set(key []byte, value []byte) error
	// Get returns the value from store
	Get(key []byte) ([]byte, error)
	// Delete remove the key from store
	Delete(key []byte) error
}

// NewStore create the raft store
func NewStore(cfg cfg.Cfg) (Store, error) {

	if cfg.Store.Memory {
		return newMemoryStore(cfg)
	}

	// TODO: need to implement
	return recover()
}
