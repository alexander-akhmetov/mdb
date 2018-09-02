package lsmt

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alexander-akhmetov/mdb/pkg/test_utils"
)

func TestMemtableFlush(t *testing.T) {
	// test that when we flush memtable to disk it writes it correctly
	// and keys are sorted
	testutils.SetUp()
	defer testutils.Teardown()

	m := newMemtable(".test/log")

	m.Set("k2", "v2")
	m.Set("k1", "v1")

	filename := ".test/dump"
	file, _ := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)

	_, err := m.Write(file)
	file.Close()

	assert.Nil(t, err)

	expData := []byte{0x0, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x2, 0x6b, 0x31, 0x76, 0x31, 0x0, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x2, 0x6b, 0x32, 0x76, 0x32}
	data := testutils.ReadFileBinary(filename)
	assert.Equal(t, expData, data)

	expDataStr := "\x00\x00\x00\x00\x02\x00\x00\x00\x02k1v1\x00\x00\x00\x00\x02\x00\x00\x00\x02k2v2"
	assert.Equal(t, expDataStr, string(data))
}

func TestNewMemtable(t *testing.T) {
	// test new memtable initialization
	testutils.SetUp()
	defer testutils.Teardown()

	f := ".test/log"
	m := newMemtable(f)

	assert.Equal(t, map[string]string{}, m.data)
	assert.Equal(t, f, m.logFilename)
}

func TestSize(t *testing.T) {
	testutils.SetUp()
	defer testutils.Teardown()

	m := newMemtable(".test/log")

	// at first size is zero
	assert.Equal(t, int64(0), m.Size())

	m.Set("k1", "v1")
	assert.Equal(t, int64(1), m.Size())

	// let's add the same key, size must be the same
	m.Set("k1", "v1")
	assert.Equal(t, int64(1), m.Size())

	// new key: size must be changed
	m.Set("k2", "v2")
	assert.Equal(t, int64(2), m.Size())
}

func TestAppendOnlyLog(t *testing.T) {
	// test that memtable will save correct data to append only log
	testutils.SetUp()
	defer testutils.Teardown()

	f := ".test/log"

	m := newMemtable(f)

	data := testutils.ReadFileBinary(f)
	assert.Equal(t, []byte{}, data)

	// add a key-value pair and check aolog
	m.Set("k", "v")

	expData := []byte{0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x1, 0x6b, 0x76}
	data = testutils.ReadFileBinary(f)
	assert.Equal(t, expData, data)

	// now let's dump this data: it must be the same
	df := ".test/dump"
	file, _ := os.OpenFile(df, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	_, err := m.Write(file)
	file.Close()
	assert.Nil(t, err)
	data = testutils.ReadFileBinary(df)
	assert.Equal(t, expData, data)

	// add a new value for the same key and check aolog
	m.Set("k", "v2")

	expData = []byte{0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x1, 0x6b, 0x76, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x2, 0x6b, 0x76, 0x32}
	data = testutils.ReadFileBinary(f)
	assert.Equal(t, expData, data)

	// now let's dump this data again:
	// it must save only the last value for the key
	os.Remove(df)
	file, _ = os.OpenFile(df, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	_, err = m.Write(file)
	file.Close()
	assert.Nil(t, err)
	data = testutils.ReadFileBinary(df)
	expData = []byte{0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x2, 0x6b, 0x76, 0x32}
	assert.Equal(t, expData, data)
}
