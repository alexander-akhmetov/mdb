package lsmt

import (
	"io"
	"log"
	"os"

	"github.com/alexander-akhmetov/mdb/pkg/lsmt/internal/entry"
	"github.com/alexander-akhmetov/mdb/pkg/lsmt/internal/rbt"
	"github.com/alexander-akhmetov/mdb/pkg/utils"
)

type ssTableConfig struct {
	readBufferSize int
	filename       string
}

const defaultReadBufferSize = 4096

type ssTable struct {
	index  *rbt.RedBlackTree
	config *ssTableConfig
}

// listSSTables returns filenames ordered by last modified time in descending order.
func listSSTables(dir string) []utils.FileInfo {
	return utils.ListFilesOrdered(dir, ".sstable")
}

func (s *ssTable) Get(key string) (string, bool) {
	offset := s.index.GetClosest(key)

	file, err := os.OpenFile(s.config.filename, os.O_RDONLY, 0600)
	if err != nil {
		log.Panicf("[ERROR]: Can't read sstable file=%s, err:%v", s.config.filename, err)
	}
	defer file.Close()

	log.Printf("[DEBUG] Reading file from offset=%v to find key=%s", offset, key)

	file.Seek(int64(offset), io.SeekStart)
	scanner := newBinFileScanner(file, s.config.readBufferSize)

	counter := 0
	for scanner.Scan() {
		counter++
		entry, _ := entry.NewDBEntry(scanner.Bytes())

		if entry.Key == key {
			log.Printf("[DEBUG] Scanned %v entries to find the key", counter)
			return entry.Value, true
		}
	}

	return "", false
}

// rebuildSparseIndex reads the entire file and builds the initial index.
func (s *ssTable) rebuildSparseIndex() {
	s.index = rbt.NewRBTree()

	file, err := os.OpenFile(s.config.filename, os.O_RDONLY, 0600)
	if err != nil {
		log.Printf("[ERROR]: Can't read sstable file=%s, err:%v", s.config.filename, err)
		return
	}
	defer file.Close()

	scanner := newBinFileScanner(file, s.config.readBufferSize)

	offset := 0
	previousKeyOffset := 0

	for scanner.Scan() {
		entry, _ := entry.NewDBEntry(scanner.Bytes())

		if s.index.Size() == 0 || offset-previousKeyOffset > s.config.readBufferSize {
			s.index.Put(entry.Key, offset)
			previousKeyOffset = offset
		}
		offset += entry.Length()
	}
}

// newSSTable returns an SSTable instance that can be used to retrieve information from this table.
func newSSTable(config *ssTableConfig) *ssTable {
	log.Println("[DEBUG] Initializing a new SSTable instance...")
	if config.readBufferSize == 0 {
		config.readBufferSize = defaultReadBufferSize
	}
	s := ssTable{
		config: config,
	}
	s.rebuildSparseIndex()
	log.Printf(
		"[DEBUG] New SSTable instance ready to use, filename=%s bufferSize=%v indexSize=%v",
		s.config.filename,
		s.config.readBufferSize,
		s.index.Size(),
	)
	return &s
}
