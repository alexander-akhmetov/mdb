package memory

import "log"

// Storage holds all in memory
type Storage struct {
	storage map[string]string
}

// Set saves given key and value
func (s *Storage) Set(key string, value string) {
	s.storage[key] = value
}

// Get returns a value by given key
func (s *Storage) Get(key string) (string, bool) {
	if value, exists := s.storage[key]; exists {
		return value, true
	}

	return "", false
}

// Delete removes key from storage
// func (s *Storage) Delete(key string) {
// 	delete(s.storage, key)
// }

// Start initializes memory storage
func (s *Storage) Start() {
	log.Println("[INFO] Starting memory storage")
	s.storage = map[string]string{}
}

// Stop stops the storage
func (s *Storage) Stop() {
}
