package lsmt

import (
	"os"
	"testing"

	"github.com/alexander-akhmetov/mdb/pkg/lsmt/internal/entry"
	"github.com/stretchr/testify/assert"

	"github.com/alexander-akhmetov/mdb/pkg/test_utils"
)

func TestAppendBinaryToFile(t *testing.T) {
	// test appendBinaryToFile
	// file must exists
	testutils.SetUp()
	defer testutils.Teardown()

	filename := ".test/test.bin"
	// let's create an empty file
	testutils.CreateFile(filename, "")

	key := "test-key"
	value := "test-value"

	testutils.AssertFileEmpty(t, filename)

	appendBinaryToFile(filename, &entry.DBEntry{
		Key:   key,
		Value: value,
	})

	// check that keys are added
	expKeysMap := [][2]string{[2]string{key, value}}
	testutils.AssertKeysInFile(t, filename, expKeysMap)

	// check binary content
	bytes := testutils.ReadFileBinary(filename)
	expBytes := []byte{0x0, 0x0, 0x0, 0x0, 0x8, 0x0, 0x0, 0x0, 0xa, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x6b, 0x65, 0x79, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x76, 0x61, 0x6c, 0x75, 0x65}
	assert.Equal(t, expBytes, bytes)
}

func TestNewBinFileScanner(t *testing.T) {
	// check that newBinFileScanner returns a new scanner
	// which can read our binary file
	testutils.SetUp()
	defer testutils.Teardown()

	filename := ".test/test.bin"
	// let's create an empty file
	testutils.CreateFile(filename, "")

	key := "test-key"
	value := "test-value"

	testutils.AssertFileEmpty(t, filename)

	appendBinaryToFile(filename, &entry.DBEntry{
		Key:   key,
		Value: value,
	})

	bytes := testutils.ReadFileBinary(filename)
	expBytes := []byte{0x0, 0x0, 0x0, 0x0, 0x8, 0x0, 0x0, 0x0, 0xa, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x6b, 0x65, 0x79, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x76, 0x61, 0x6c, 0x75, 0x65}
	assert.Equal(t, expBytes, bytes)

	f, _ := os.Open(filename)
	readBufferSize := 1024
	scanner := newBinFileScanner(f, readBufferSize)

	// we have only one key-value in the file
	e, err := scanner.ReadEntry()
	assert.Nil(t, err)
	assert.Equal(t, &entry.DBEntry{Key: key, Value: value}, e)

	// next read must be empty and return an error
	e, err = scanner.ReadEntry()
	assert.Equal(t, &entry.IncompleteEntryError{}, err)
	assert.Equal(t, &entry.DBEntry{Key: "", Value: ""}, e)
}
