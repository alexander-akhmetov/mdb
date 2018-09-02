package indexedfile

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/alexander-akhmetov/mdb/pkg/utils"
)

var writeMutex = &sync.Mutex{}

// Storage holds all in a file
type Storage struct {
	Filename string
	index    map[string]int64
}

// Set saves given key and value
func (s *Storage) Set(key string, value string) {
	writeMutex.Lock()
	defer writeMutex.Unlock()

	strToAppend := fmt.Sprintf("%s;%s\n", key, value)
	s.index[key] = utils.GetFileSize(s.Filename)
	log.Printf("[DEBUG] Adding key=%s with indexOffset=%v", key, s.index[key])
	utils.AppendToFile(s.Filename, strToAppend)
}

// Get returns a value by given key and boolean indicator that key exists
func (s *Storage) Get(key string) (string, bool) {
	var line string
	if offset, ok := s.index[key]; ok {
		log.Printf("[DEBUG] Reading key=%s with indexOffset=%v", key, offset)
		line = utils.ReadLineByOffset(s.Filename, offset)
		return utils.TrimKey(key, line), true
	}

	return "", false
}

// Start initializes Storage, creates file if needed and rebuilds index
func (s *Storage) Start() {
	log.Println("[INFO] Starting indexed file storage")
	utils.StartFileDB()
	utils.CreateFileIfNotExists(s.Filename)
	log.Println("[DEBUG] Storage: rebuilding index...")
	s.rebuildIndex()
	log.Println("[DEBUG] Storage: started")
}

// rebuildIndex reads all file and builds initial index
// it will be very slow for large files
func (s *Storage) rebuildIndex() {
	s.index = map[string]int64{}

	file, err := os.OpenFile(s.Filename, os.O_RDONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	offset := int64(0)

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		key := strings.Split(line, ";")[0]
		s.index[key] = offset
		offset += int64(len([]byte(line)) + 1)
	}
}

// Stop stops the storage
func (s *Storage) Stop() {
	utils.StopFileDB()
}
