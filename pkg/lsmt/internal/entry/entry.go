package entry

import (
	"encoding/binary"
	"io"
)

// DBEntry represents a one database entry
type DBEntry struct {
	Type  uint8 // 0: simple value
	Key   string
	Value string
}

// IncompleteEntryError is an Error which indicates that binary data
// does not contain an entry
type IncompleteEntryError struct{}

func (e *IncompleteEntryError) Error() string {
	return "Incomplete entry"
}

// NewDBEntry returns a new DBEntry structure
// it parses incoming data and builds key and value from it
func NewDBEntry(data []byte) (*DBEntry, error) {
	if len(data) < 9 {
		return &DBEntry{}, &IncompleteEntryError{}
	}

	keyLength := binary.BigEndian.Uint32(data[1:5])
	valueLength := binary.BigEndian.Uint32(data[5:9])

	dataLength := uint32(len(data))
	expDataLength := 9 + keyLength + valueLength
	if dataLength < expDataLength {
		return &DBEntry{}, &IncompleteEntryError{}
	}

	entry := DBEntry{
		Type:  0,
		Key:   string(data[9 : 9+keyLength]),
		Value: string(data[9+keyLength : 9+keyLength+valueLength]),
	}

	return &entry, nil
}

// Length returns full length of the entry in binary format
func (e *DBEntry) Length() int {
	return len(e.Binary())
}

// Binary returns byte array with all data of the entry
// including key and value lengths
func (e *DBEntry) Binary() []byte {
	bkey := []byte(e.Key)
	bvalue := []byte(e.Value)

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

	return data
}

// Write writes binary representation of the entry to io.Writer
func (e *DBEntry) Write(w io.Writer) (n int, err error) {
	return w.Write(e.Binary())
}
