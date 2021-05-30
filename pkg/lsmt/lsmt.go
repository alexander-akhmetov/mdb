package lsmt

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alexander-akhmetov/mdb/pkg/utils"
)

const defaultMaxMemtableSize int64 = 256
const defaultMaxCompactFileSize int64 = 1024 * 1024 * 10

// Prevents changing the memtablesFlushQueue
var flushMutex = &sync.Mutex{}

// Prevents changing the ssTables list
var ssTablesListMutex = &sync.Mutex{}

// Locks access to the ssTables list
var ssTablesAccessMutex = &sync.Mutex{}

// StorageConfig holds all configuration of the storage
type StorageConfig struct {
	WorkDir string

	CompactionEnabled     bool
	MinimumFilesToCompact int
	MaxMemtableSize       int64
	MaxCompactFileSize    int64
	SSTableReadBufferSize int

	pidFilePath          string
	memtablesFlushTmpDir string
	aoLogPath            string
	ssTablesDir          string
	tmpDir               string
}

// Storage holds data in ss tables
type Storage struct {
	Config StorageConfig

	running             bool
	memtable            *memtable
	ssTables            []*ssTable
	memtablesFlushQueue []*memtable
}

// Set saves the given key and value.
func (s *Storage) Set(key string, value string) {
	s.flushmemtableIfNeeded()
	s.memtable.Set(key, value)
}

// flushmemtableIfNeeded checks if the memtable is bigger than the limit size and puts it into the flush queue if yes.
func (s *Storage) flushmemtableIfNeeded() {
	if s.memtable.Size() > s.Config.MaxMemtableSize {
		log.Println("[DEBUG] memtable is too big: putting it to flush queue")

		memtable := s.memtable
		memtable.timestamp = time.Now().UnixNano()
		newLogPath := filepath.Join(
			s.Config.memtablesFlushTmpDir,
			fmt.Sprintf("%v.aolog", memtable.timestamp),
		)
		log.Println("[DEBUG] Moving AOLog to a new path=", newLogPath)
		os.Rename(memtable.logFilename, newLogPath)

		s.initNewMemtable()

		memtable.logFilename = newLogPath
		go s.appendToFlushQueue(memtable)
	}
}

// appendToFlushQueue inserts wmemtable into the memtablesFlushQueue at the first place (prepend).
// We need to keep the memtablesFlushQueue ordered by memtable age (descending order: newest first),
// so we will check memtables from the beginning if we want to find some key.
func (s *Storage) appendToFlushQueue(m *memtable) {
	// we must lock this mutex to obtain exclusive access to the flush queue
	flushMutex.Lock()
	defer flushMutex.Unlock()

	// this memtable is newer than other in the memtablesFlushQueue
	// so put it to the beginning of the queue
	s.memtablesFlushQueue = append([]*memtable{m}, s.memtablesFlushQueue...)
}

// Get returns a value for the given key and a boolean indicator of whether the key exists.
func (s *Storage) Get(key string) (value string, exists bool) {
	value, exists = s.memtable.Get(key)

	if !exists {
		log.Printf("[DEBUG] key=%s has NOT been found in the memtable, searching in the FlushQueue...", key)
		value, exists = s.getFromFlushQueue(key)
	}

	if !exists {
		log.Printf("[DEBUG] key=%s has NOT been found in the FlushQueue, searching in the SSTables...", key)
		value, exists = s.getFromSSTables(key)
	}

	return value, exists
}

// getFromFlushQueue tries to find the given key in the flush queue memtables.
func (s *Storage) getFromFlushQueue(key string) (string, bool) {
	value := ""
	found := false

	for _, fq := range s.memtablesFlushQueue {
		value, found = fq.Get(key)
		if found {
			log.Printf("[DEBUG] key=%s has been found in the flush queue=%v", key, fq.timestamp)
			return value, found
		}
	}

	log.Printf("[DEBUG] key=%s has NOT been found in the flush queue", key)

	return value, found
}

// getFromSSTables tries to find the given key in the SSTables.
// It searches for keys in parallel in all SSTables.
func (s *Storage) getFromSSTables(key string) (string, bool) {
	value := ""
	found := false

	ssTablesAccessMutex.Lock()
	defer ssTablesAccessMutex.Unlock()

	type result struct {
		position int
		value    string
	}

	queue := make(chan result, len(s.ssTables))

	var wg sync.WaitGroup
	wg.Add(len(s.ssTables))

	for i, st := range s.ssTables {
		go func(i int, st *ssTable) {
			defer wg.Done()
			value, found = st.Get(key)
			if found {
				queue <- result{position: i, value: value}
			}
		}(i, st)
	}

	wg.Wait()
	close(queue)

	foundAt := len(s.ssTables)
	for elem := range queue {
		if elem.position <= foundAt {
			value = elem.value
			found = true
			foundAt = elem.position
		}
	}

	if !found {
		log.Printf("[DEBUG] key=%s has NOT been found in the sstables", key)
	}

	return value, found
}

// Start initializes Storage
func (s *Storage) Start() {
	log.Println("[INFO] Starting lsmt storage")

	if s.Config.MaxMemtableSize == 0 {
		s.Config.MaxMemtableSize = defaultMaxMemtableSize
	}
	if s.Config.MaxCompactFileSize == 0 {
		s.Config.MaxCompactFileSize = defaultMaxCompactFileSize
	}

	if s.Config.MinimumFilesToCompact == 0 {
		s.Config.MinimumFilesToCompact = 2
	}

	s.Config.memtablesFlushTmpDir = filepath.Join(s.Config.WorkDir, "aolog_tf")
	s.Config.aoLogPath = filepath.Join(s.Config.WorkDir, "log.aolog")
	s.Config.ssTablesDir = filepath.Join(s.Config.WorkDir, "sstables")
	s.Config.tmpDir = filepath.Join(s.Config.WorkDir, "tmp")
	s.Config.pidFilePath = filepath.Join(s.Config.WorkDir, "mdb.pid")

	s.createWorkDirs()
	utils.CheckAndCreatePIDFile(s.Config.pidFilePath)

	os.RemoveAll(s.Config.tmpDir) // clean tmp dir

	s.restoreSSTables()
	s.restoreFlushQueue()
	s.initNewMemtable()

	s.running = true
	go s.startFlusherProcess()

	if s.Config.CompactionEnabled {
		go s.startCompactionProcess()
	} else {
		log.Println("[DEBUG] Compaction disabled")
	}

	log.Println("[INFO] Storage ready")
}

// restoreFlushQueue reads the flush queue directory and restores memtables
// from files (aolog) in this directory to the memtablesFlushQueue.
func (s *Storage) restoreFlushQueue() {
	log.Println("[DEBUG] Restoring flush queue...")
	files := utils.ListFilesOrdered(s.Config.memtablesFlushTmpDir, "")
	for _, f := range files {
		log.Println("[DEBUG] Found flush queue alog = ", f.Name)

		wb := newMemtable(f.Name)
		timestamp, err := strconv.ParseInt(strings.Split(filepath.Base(f.Name), ".")[0], 10, 64)
		if err != nil {
			log.Panic("[ERROR] Can not read flush queue file = ", f.Name, err)
		}
		wb.timestamp = timestamp
		// files are already ordered by name in descending order, put this file to the end of the list
		s.memtablesFlushQueue = append(s.memtablesFlushQueue, wb)
	}
	log.Println("[DEBUG] Flush queue has been restored with size=", len(s.memtablesFlushQueue))
}

// initNewMemtable initializes a new memtable for the storage.
func (s *Storage) initNewMemtable() {
	s.memtable = newMemtable(s.Config.aoLogPath)
}

// createWorkDirs creates the necessary directories.
func (s *Storage) createWorkDirs() {
	dirs := []string{s.Config.ssTablesDir, s.Config.memtablesFlushTmpDir, s.Config.tmpDir}
	for _, dir := range dirs {
		log.Println("[DEBUG] Creating dir", dir)
		utils.CreateDir(dir)
	}
}

// restoreSSTables reads the directory with SSTables and restores them to the `ssTables` attribute.
func (s *Storage) restoreSSTables() {

	type result struct {
		position int
		table    *ssTable
	}

	tablesToRestore := listSSTables(s.Config.ssTablesDir)

	// since ssTables is nil before here
	s.ssTables = make([]*ssTable, len(tablesToRestore))

	var wg sync.WaitGroup
	wg.Add(len(tablesToRestore))

	// initialize ssTables in parallel
	for i, file := range tablesToRestore {
		// Files are already ordered by name in descending order.
		// Later, we will put this file at the end of the list.
		go func(position int, filename string) {
			defer wg.Done()
			s.ssTables[position] = newSSTable(
				&ssTableConfig{
					filename:       filename,
					readBufferSize: s.Config.SSTableReadBufferSize,
				},
			)
		}(i, file.Name)
	}

	wg.Wait()

	log.Println("[DEBUG] initialized sstables:", len(s.ssTables))
}

// startFlusherProcess starts the flusher process, which checks
// if we need to flush some memtable and flushes it if needed.
func (s *Storage) startFlusherProcess() {
	log.Println("[DEBUG] Started flusher process")
	for s.running == true {
		// Lock the mutex so that no new memtables are added
		// while we are dumping memtables to disk.
		// This ensures that we can flush the entire queue and clean it.
		flushMutex.Lock()

		// FIFO: We iterate in reverse order to dump the oldest memtables to disk first.
		// This allows us to serve read requests correctly: we search in the main memtable first,
		// then in the "memtables to flush" queue from top to bottom (newest first),
		// and finally in SSTables.
		for i := len(s.memtablesFlushQueue) - 1; i >= 0; i-- {
			f := newFlusher(s.memtablesFlushQueue[i], s.Config.ssTablesDir)
			filename := f.flush()
			// It is the newest SSTable, so put it at the beginning of the list.
			ssTablesListMutex.Lock()
			newt := newSSTable(
				&ssTableConfig{
					filename:       filename,
					readBufferSize: s.Config.SSTableReadBufferSize,
				},
			)
			s.ssTables = append([]*ssTable{newt}, s.ssTables...)
			ssTablesListMutex.Unlock()
		}

		// Clean the flush queue since we flushed all memtables and
		// the mutex prevents other goroutines from adding new items to this queue.
		s.memtablesFlushQueue = []*memtable{}

		// Unlock the mutex and sleep for some time.
		flushMutex.Unlock()
		time.Sleep(time.Millisecond * 100)
	}
}

func (s *Storage) startCompactionProcess() {
	log.Println("[DEBUG] Started compaction process")

	// We need to merge two files together and place the result file in the temporary directory.
	// Then we lock ssTables to ensure exclusive access to change it,
	// and move the result file to the location of the second merged one.
	// We do this because the second file is newer,
	// and even if something goes wrong, we won't lose data.
	//
	// After moving the result file, we can remove the first merged file as we don't need it anymore.
	// Then we remove its ssTable instance from the list.
	// However, we already don't use it automatically since all newer keys are in the newer file.
	for s.running == true {
		firstMerged, secondMerged, resultFile, isMerged := compact(
			s.Config.ssTablesDir,
			s.Config.tmpDir,
			s.Config.MinimumFilesToCompact,
			s.Config.MaxCompactFileSize,
		)
		if isMerged {
			ssTablesListMutex.Lock()

			// find ssTables which we need to remove
			firstIndex := s.findSSTableIndex(firstMerged)
			secondIndex := s.findSSTableIndex(secondMerged)

			// initiate it to pre-build index
			newSSTable := newSSTable(
				&ssTableConfig{
					filename:       resultFile,
					readBufferSize: s.Config.SSTableReadBufferSize,
				},
			)

			file, _ := os.Open(resultFile)
			defer file.Close()

			ssTablesAccessMutex.Lock()
			// Move the result file to the location of the second merged file.
			err := os.Rename(resultFile, secondMerged)
			if err != nil {
				log.Printf("[ERROR] Can't move merged file from '%s' to '%s': %v", resultFile, secondMerged, err)
				ssTablesAccessMutex.Unlock()
				continue
			}
			s.ssTables[secondIndex].index = newSSTable.index

			// remove the first merged file
			// https://github.com/golang/go/wiki/SliceTricks : delete without memory leak
			copy(s.ssTables[firstIndex:], s.ssTables[firstIndex+1:])
			s.ssTables[len(s.ssTables)-1] = nil
			s.ssTables = s.ssTables[:len(s.ssTables)-1]

			ssTablesAccessMutex.Unlock()

			err = os.Remove(firstMerged)
			if err != nil {
				log.Printf("[ERROR] Can't remove merged file from '%s': %v", firstMerged, err)
				ssTablesListMutex.Unlock()
				continue
			}
			ssTablesListMutex.Unlock()
			log.Println("[DEBUG] Compaction completed")
		} else {
			// If we didn't merge files, let's sleep.
			// But if we just merged files, we want to check if we need to merge them again.
			time.Sleep(time.Millisecond * 100)
		}
	}
}

// findSSTableIndex returns the index of an SSTable in the ssTables list.
func (s *Storage) findSSTableIndex(filename string) int {
	index := -1

	for i, t := range s.ssTables {
		if t.config.filename == filename {
			index = i
			break
		}
	}

	return index
}

// Stop stops the storage
func (s *Storage) Stop() {
	utils.RemovePIDFile(s.Config.pidFilePath)
	s.running = false
}
