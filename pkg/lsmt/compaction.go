package lsmt

import (
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/alexander-akhmetov/mdb/pkg/utils"
)

var compactionMutex = &sync.Mutex{}

const ssTableReadBufferSize = 4096

// compact finds N SSTables in the workDir,
// which are can be merged together (they are must be smaller some limit)
// and merges them into a one bigger SSTable, then it removes old files
func compact(workDir string, tmpDir string, minimumFilesToCompact int, maxCompactFileSize int64) (string, string, string, bool) {
	compactionMutex.Lock()
	defer compactionMutex.Unlock()

	fFile, sFile, needToCompact := getTwoFilesToCompact(workDir, minimumFilesToCompact, maxCompactFileSize)

	if !needToCompact {
		return "", "", "", false
	}
	log.Println("[DEBUG] Started compaction process")

	tmpFilePath := filepath.Join(tmpDir, filepath.Base(sFile))
	utils.CreateFileIfNotExists(tmpFilePath)

	merge(fFile, sFile, tmpFilePath)

	return fFile, sFile, tmpFilePath, true
}

// merge merges files into a one
func merge(fFile string, sFile string, mergeTo string) {
	log.Printf("[DEBUG] Merging %s + %s => %s", fFile, sFile, mergeTo)

	firstFile, err := os.Open(fFile)
	if err != nil {
		log.Println("[ERROR] Can't open file to compact = ", fFile)
	}
	defer firstFile.Close()

	secondFile, err := os.Open(sFile)
	if err != nil {
		log.Println("[ERROR] Can't open file to compact = ", sFile)
	}
	defer secondFile.Close()

	firstScanner := newBinFileScanner(firstFile, ssTableReadBufferSize)
	secondScanner := newBinFileScanner(secondFile, ssTableReadBufferSize)

	fEntry, _ := firstScanner.ReadEntry()
	sEntry, _ := secondScanner.ReadEntry()

	for true == true {
		// compare files line by line and add to the new file only last keys
		for (sEntry.Key > fEntry.Key && fEntry.Key != "") || (fEntry.Key != "" && sEntry.Key == "") {
			appendBinaryToFile(mergeTo, fEntry)
			fEntry, _ = firstScanner.ReadEntry()
		}

		for (sEntry.Key <= fEntry.Key && sEntry.Key != "") || (fEntry.Key == "" && sEntry.Key != "") {
			appendBinaryToFile(mergeTo, sEntry)
			for sEntry.Key == fEntry.Key {
				// if keys are equal, we need to read next first key too,
				// otherwise we will save it again in this loop
				fEntry, _ = firstScanner.ReadEntry()
			}
			sEntry, _ = secondScanner.ReadEntry()
		}
		if fEntry.Key == "" && sEntry.Key == "" {
			break
		}
	}
}

// getTwoFilesToCompact returns paths to to files which we can merge
// and boolean third argument which indicates can we merge files or not
func getTwoFilesToCompact(dir string, minimumFilesToCompact int, maxFileSize int64) (string, string, bool) {
	allFiles := listSSTables(dir)

	// filter big files
	files := []utils.FileInfo{}
	for _, f := range allFiles {
		if f.Size < maxFileSize {
			files = append(files, f)
		}
	}
	filesCount := len(files)

	if filesCount < minimumFilesToCompact {
		return "", "", false
	}

	firstFileInfo := files[filesCount-1]
	secondFileInfo := files[filesCount-2]

	return firstFileInfo.Name, secondFileInfo.Name, true
}
