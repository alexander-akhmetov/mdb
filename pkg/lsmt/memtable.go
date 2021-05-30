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
	data        map[string]string // In-memory data structure to keep information before saving to disk as SSTable.
	logFilename string            // AOLog: append-only log to restore information in case of a crash.
	timestamp   int64             // Used for the flush process.
}

// Set writes information to AOLog.
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

// Get returns the value of a key from the memtable.
func (m *memtable) Get(key string) (string, bool) {
	if value, ok := m.data[key]; ok {
		return value, true
	}

	return "", false
}

// Size returns the size of a memtable in bytes.
// It's needed to decide if we need to dump this memtable to disk as an SSTable or not.
func (m *memtable) Size() int64 {
	return int64(len(m.data))
}

// restoreFromLog reads the AOLog file and restores all information back to the memtable.
// We use it in case of a crash or when the server was stopped with some information in the memtable.
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

// newMemtable returns a new instance of a writer.
func newMemtable(aoLogFileName string) *memtable {
	m := &memtable{
		data:        map[string]string{},
		logFilename: aoLogFileName,
	}
	m.restoreFromLog()
	return m
}
