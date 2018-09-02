package testutils

import (
	"encoding/binary"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// SetUp cleans all before the test
func SetUp() {
	clean()
}

// Teardown removes all test data from disk
func Teardown() {
	clean()
}

func clean() {
	os.RemoveAll(".test")
	os.Remove("mdb.pid")
	os.Remove("lsmt_test")
}

// ReadFile reads file to a memory
func ReadFile(filename string) string {
	return string(ReadFileBinary(filename))
}

// ReadFileBinary reads file to a memory as a byte array
func ReadFileBinary(filename string) []byte {
	file, _ := os.Open(filename)
	defer file.Close()

	b, _ := ioutil.ReadAll(file)
	return b
}

// IsFileExists returns file existance status
func IsFileExists(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}

// CreateFile creates file with all necessary dirs and given content
func CreateFile(path string, content string) {
	os.RemoveAll(path)

	dir, _ := filepath.Split(path)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.ModePerm)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Panic(err)
	}
	defer f.Close()

	_, err = f.WriteString(content)
	if err != nil {
		log.Panic(err)
	}
}

// IsDirEmpty returns true if the dir is empty
func IsDirEmpty(path string) bool {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Panic(err)
	}
	return len(files) == 0
}

// CreateFileWithKeyValues creates file with binary key values
func CreateFileWithKeyValues(filename string, keyValues [][2]string) {
	os.RemoveAll(filename)

	dir, _ := filepath.Split(filename)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.ModePerm)
	}

	file, _ := os.OpenFile(filename, os.O_CREATE, 0600)
	file.Close()

	for _, kv := range keyValues {
		appendBinaryToFile(filename, kv[0], kv[1])
	}
}

// appendBinaryToFile writes key-value in binary format
func appendBinaryToFile(filename string, key string, value string) {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	bkey := []byte(key)
	bvalue := []byte(value)

	keyLength := make([]byte, 4)
	binary.BigEndian.PutUint32(
		keyLength,
		uint32(len(bkey)),
	)
	valueLength := make([]byte, 4)
	binary.BigEndian.PutUint32(
		valueLength,
		uint32(len(bvalue)),
	)

	data := []byte{0}
	for _, b := range [][]byte{keyLength, valueLength, bkey, bvalue} {
		data = append(data, b...)
	}

	_, err = file.Write(data)
	if err != nil {
		log.Panic(err)
	}
}

// AssertKeysInFile checks that file's content equals expected data
func AssertKeysInFile(t *testing.T, filename string, data [][2]string) {
	expContent := []byte{}

	for _, kv := range data {
		keyLength := make([]byte, 4)
		binary.BigEndian.PutUint32(
			keyLength,
			uint32(len(kv[0])),
		)
		valueLength := make([]byte, 4)
		binary.BigEndian.PutUint32(
			valueLength,
			uint32(len(kv[1])),
		)
		expContent = append(expContent, []byte{0}...)
		expContent = append(expContent, keyLength...)
		expContent = append(expContent, valueLength...)
		expContent = append(expContent, []byte(kv[0])...)
		expContent = append(expContent, []byte(kv[1])...)
	}

	content := ReadFileBinary(filename)
	assert.Equal(t, expContent, content)
}

// AssertFileEmpty checks that file is emtpy
func AssertFileEmpty(t *testing.T, filename string) {
	assert.Equal(t, []byte{}, ReadFileBinary(filename))
}
