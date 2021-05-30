package memory

import "log"

// Storage holds data in memory
type Storage struct {
	storage map[string]string
}

// Set saves the given key and value.
func (s *Storage) Set(key string, value string) {
	s.storage[key] = value
}

// Get returns a value for the given key.
func (s *Storage) Get(key string) (string, bool) {
	if value, exists := s.storage[key]; exists {
		return value, true
	}

	return "", false
}

// Start initializes the memory storage
func (s *Storage) Start() {
	log.Println("[INFO] Starting memory storage")
	s.storage = map[string]string{}
}

// Stop stops the storage
func (s *Storage) Stop() {
}
