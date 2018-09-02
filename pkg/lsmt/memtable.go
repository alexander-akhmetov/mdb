package lsmt

import (
	"io"
	"log"
	"os"
	"sort"
	"sync"

	"github.com/alexander-akhmetov/mdb/pkg/lsmt/internal/entry"
	"github.com/alexander-akhmetov/mdb/pkg/utils"
)

var writeMutex = &sync.Mutex{}

const aoLogReadBufferSize = 4096

type memtable struct {
	data        map[string]string // in-memory data structure to keep info before saving to disk as SSTable
	logFilename string            // AOLog: append-only log to restore information in case of crash
	timestamp   int64             //used for flush process
}

// set writes informartion to AOLog
func (m *memtable) Set(key string, value string) {
	m.appendToLog(key, value)
	m.data[key] = value
}

// appendToLog appends binary data to AOLog
func (m *memtable) appendToLog(key string, value string) {
	writeMutex.Lock()
	defer writeMutex.Unlock()

	log.Printf("[DEBUG] Adding key=%s to AOLog", key)
	appendBinaryToFile(m.logFilename, &entry.DBEntry{
		Type:  0,
		Key:   key,
		Value: value,
	})
}

// get returns value of a key from memtable
func (m *memtable) Get(key string) (string, bool) {
	if value, ok := m.data[key]; ok {
		return value, true
	}

	return "", false
}

// getSize returns size of a memtable in bytes
// it's needed to decide if we need to dump this memtable to a disk as SSTable or not
func (m *memtable) Size() int64 {
	return int64(len(m.data))
}

// restoreFromLog reads AOLog file and restores all information back to the memtable
// we use it in case of crash or when server was stopped with some informartion in the memtable
func (m *memtable) restoreFromLog() {
	file, err := os.OpenFile(m.logFilename, os.O_RDONLY, 0600)
	if os.IsNotExist(err) {
		// if file doesn't exist - it's a new memtable
		log.Println("[DEBUG] AOLog file does not exist, skipping restoring process")
		utils.CreateFileIfNotExists(m.logFilename)
		return
	}

	log.Println("[DEBUG] AOLog file exists, restoring...")

	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := newBinFileScanner(file, aoLogReadBufferSize)

	for scanner.Scan() {
		entry, _ := entry.NewDBEntry(scanner.Bytes())
		m.data[entry.Key] = entry.Value
	}
	counter := len(m.data)
	log.Printf("[DEBUG] Restored %v entries", counter)
}

// Write writes binary representation of the memtable to io.Writer
func (m *memtable) Write(wr io.Writer) (n int, err error) {
	result := []*entry.DBEntry{}
	for key, value := range m.data {
		result = append(result, &entry.DBEntry{
			Key:   key,
			Value: value,
			Type:  0,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	for _, entry := range result {
		written, err := entry.Write(wr)
		if err != nil {
			return n, err
		}
		n += written
	}

	return n, err
}

// newMemtable returns new instance of a writer
func newMemtable(aoLogFileName string) *memtable {
	m := &memtable{
		data:        map[string]string{},
		logFilename: aoLogFileName,
	}
	m.restoreFromLog()
	return m
}
