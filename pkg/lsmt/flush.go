package lsmt

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/alexander-akhmetov/mdb/pkg/utils"
)

// flusher is a struct which holds information about
// memtable which we flush to a disk
type flusher struct {
	sstablesDir string
	memtable    *memtable
}

// flush dumps data from flusher.memtable to a new SSTable on a disk
// SSTable's name defined as a "{flusher.timestamp}.sstable"
func (f *flusher) flush() string {
	log.Printf("[DEBUG] Starting memtable flushing process for aolog=%s", f.memtable.logFilename)
	file, err := os.OpenFile(f.filename(), os.O_APPEND|os.O_WRONLY, filePermissions)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	_, err = f.memtable.Write(file)
	if err != nil {
		log.Panic(err)
	}
	err = file.Sync()
	if err != nil {
		log.Panic(err)
	}

	log.Printf("[DEBUG] Removing old append only log file at path=%s", f.memtable.logFilename)
	err = os.Remove(f.memtable.logFilename)
	if err != nil {
		log.Panicf("[ERROR] Can't remove old log file at=%s, err=%v", f.memtable.logFilename, err)
	}

	log.Printf("[DEBUG] memtable saved as SSTable to the file=%s", file.Name())

	return file.Name()
}

// filename returns full path to an SSTable file
// in which flusher writes memtable's data
func (f *flusher) filename() string {
	return filepath.Join(
		f.sstablesDir,
		fmt.Sprintf("%v.sstable", f.memtable.timestamp),
	)
}

// newFlusher returns a new flusher instance
func newFlusher(memtable *memtable, workDir string) *flusher {
	f := flusher{
		memtable:    memtable,
		sstablesDir: workDir,
	}
	utils.RecreateFile(f.filename())
	return &f
}
