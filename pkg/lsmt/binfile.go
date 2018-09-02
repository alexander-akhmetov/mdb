package lsmt

import (
	"bufio"
	"log"
	"os"

	"github.com/alexander-akhmetov/mdb/pkg/lsmt/internal/entry"
)

const filePermissions = 0600

// binScanner scans binary file and splits data into
// entry.DBEntry automatically
type binScanner struct {
	*bufio.Scanner
}

// appendBinaryToFile writes key-value in binary format
func appendBinaryToFile(filename string, entry *entry.DBEntry) {
	// todo: move to entry
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, filePermissions)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	_, err = entry.Write(file)

	if err != nil {
		log.Panic(err)
	}
}

func newBinFileScanner(file *os.File, readBufferSize int) *binScanner {
	scanner := bufio.NewScanner(file)
	split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		entry, err := entry.NewDBEntry(data)
		if err == nil {
			length := entry.Length()
			return length, data[:length], nil
		}

		return 0, nil, nil
	}

	buf := make([]byte, readBufferSize)
	// we will need to change buffer's maximum capacity here later
	scanner.Buffer(buf, bufio.MaxScanTokenSize)

	// set up custom split function
	scanner.Split(split)
	return &binScanner{scanner}
}

func (b *binScanner) ReadEntry() (*entry.DBEntry, error) {
	b.Scanner.Scan()
	return entry.NewDBEntry(b.Scanner.Bytes())
}
