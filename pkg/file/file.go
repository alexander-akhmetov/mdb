package file

import (
	"fmt"
	"log"
	"sync"

	"github.com/alexander-akhmetov/mdb/pkg/utils"
)

var writeMutex = &sync.Mutex{}

// Storage holds all information in a file.
type Storage struct {
	Filename string
}

// Set saves the given key and value.
func (s *Storage) Set(key string, value string) {
	writeMutex.Lock()
	defer writeMutex.Unlock()
	strToAppend := fmt.Sprintf("%s;%s\n", key, value)
	utils.AppendToFile(s.Filename, strToAppend)
}

// Get returns a value for a given key and a boolean indicator of whether the key exists.
func (s *Storage) Get(key string) (string, bool) {
	line, found := utils.FindLineByKeyInFile(s.Filename, key)
	if found {
		return utils.TrimKey(key, line), true
	}

	return "", false
}

// Start initializes Storage and creates a file if needed.
func (s *Storage) Start() {
	log.Println("[INFO] Starting file storage")
	utils.StartFileDB()
	utils.CreateFileIfNotExists(s.Filename)
}

// Stop stops the storage
func (s *Storage) Stop() {
	utils.StopFileDB()
}
