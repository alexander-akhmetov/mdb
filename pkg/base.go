// Package mdb is a simple key-value database
package mdb

import (
	"github.com/alexander-akhmetov/mdb/pkg/file"
	"github.com/alexander-akhmetov/mdb/pkg/indexed_file"
	"github.com/alexander-akhmetov/mdb/pkg/lsmt"
	"github.com/alexander-akhmetov/mdb/pkg/memory"
)

// Storage is a common interface for all storages
type Storage interface {
	Set(string, string)
	Get(string) (string, bool)
	// Delete(string)
	Start()
	Stop()
}

// NewFileStorage creates a new file.Storage
func NewFileStorage(filepath string) (storage Storage) {
	storage = &file.Storage{
		Filename: filepath,
	}
	storage.Start()
	return storage
}

// NewMemoryStorage creates a new memory.Storage
func NewMemoryStorage(filepath string) (storage Storage) {
	storage = &memory.Storage{}
	storage.Start()
	return storage
}

// NewIndexedFileStorage returns a new indexedfile.Storage
func NewIndexedFileStorage(filepath string) (storage Storage) {
	storage = &indexedfile.Storage{
		Filename: filepath,
	}
	storage.Start()
	return storage
}

// NewLSMTStorage returns a new lsmt.Storage
func NewLSMTStorage(config lsmt.StorageConfig) (storage Storage) {
	storage = &lsmt.Storage{
		Config: config,
	}
	storage.Start()
	return storage
}
