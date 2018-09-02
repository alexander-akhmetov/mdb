package entry

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBinary(t *testing.T) {
	// test that Binary() returns correct
	// binary representation of an entry
	e := &DBEntry{
		Type:  0,
		Key:   "key",
		Value: "value",
	}

	expBinary := []byte{0, 0, 0, 0, 3, 0, 0, 0, 5}
	expBinary = append(expBinary, []byte(e.Key)...)
	expBinary = append(expBinary, []byte(e.Value)...)
	assert.Equal(t, expBinary, e.Binary())
}

func TestBinaryEmptyKeyValue(t *testing.T) {
	// test Binary() with empty key and value
	e := &DBEntry{
		Type:  0,
		Key:   "",
		Value: "",
	}

	expBinary := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0}
	expBinary = append(expBinary, []byte(e.Key)...)
	expBinary = append(expBinary, []byte(e.Value)...)
	assert.Equal(t, expBinary, e.Binary())
}

func TestNewDBEntryFromBinary(t *testing.T) {
	// test initialization of DBEntry
	entryType := []byte{0}
	keyLength := []byte{0, 0, 0, 3}
	valueLength := []byte{0, 0, 0, 5}
	key := "key"
	value := "value"

	data := append(entryType, keyLength...)
	data = append(data, valueLength...)
	data = append(data, []byte(key)...)
	data = append(data, []byte(value)...)

	expEntry := DBEntry{
		Type:  0,
		Key:   key,
		Value: value,
	}

	readedEntry, err := NewDBEntry(data)

	assert.Equal(t, &expEntry, readedEntry)
	assert.Nil(t, err)
}

func TestNewDBEntryWithZeroLength(t *testing.T) {
	// if we passed incorrect binary data, NewDBEntry
	// must return IncompleteEntryError
	expEntry := DBEntry{}
	readedEntry, err := NewDBEntry([]byte{})

	assert.Equal(t, &expEntry, readedEntry)
	assert.IsType(t, &IncompleteEntryError{}, err)
}

func TestNewDBEntryFromBinaryWithoutKeyLength(t *testing.T) {
	expEntry := DBEntry{}
	readedEntry, err := NewDBEntry([]byte{0, 0, 0})

	assert.Equal(t, &expEntry, readedEntry)
	assert.IsType(t, &IncompleteEntryError{}, err)
}

func TestNewDBEntryFromBinaryWithoutValueLength(t *testing.T) {
	expEntry := DBEntry{}
	readedEntry, err := NewDBEntry([]byte{0, 0, 0, 0, 0, 0})

	assert.Equal(t, &expEntry, readedEntry)
	assert.IsType(t, &IncompleteEntryError{}, err)
}

func TestNewDBEntryFromBinaryWithIncompleteEntry(t *testing.T) {
	expEntry := DBEntry{}
	readedEntry, err := NewDBEntry([]byte{0, 0, 0, 0, 1, 0, 0, 0, 1})

	assert.Equal(t, &expEntry, readedEntry)
	assert.IsType(t, &IncompleteEntryError{}, err)
}
