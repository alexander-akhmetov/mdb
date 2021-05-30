package indexedfile

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alexander-akhmetov/mdb/pkg/test_utils"
)

func TestIndexedFileStorage(t *testing.T) {
	testutils.SetUp()
	defer testutils.Teardown()

	filename := ".test/db.mdb"
	storage := &Storage{
		Filename: filename,
	}
	storage.Start()

	testKey := "t_key"
	testValue := "t_value"
	testKey2 := "t_key_2"
	testValue2 := "t_value_2"
	storage.Set(testKey, testValue)
	storage.Set(testKey2, testValue2)

	content := testutils.ReadFile(filename)
	expContent := fmt.Sprintf("%s;%s\n%s;%s\n", testKey, testValue, testKey2, testValue2)

	assert.Equal(t, expContent, content, "File content wrong")

	// Let's read the content of this file

	value, exists := storage.Get(testKey)
	assert.Equal(t, testValue, value, "Wrong value")
	assert.True(t, exists)

	value, exists = storage.Get(testKey2)
	assert.Equal(t, testValue2, value, "Wrong value")
	assert.True(t, exists)

	assert.Equal(t, int64(0), storage.index[testKey], "")
	secondOffset := len(fmt.Sprintf("%s;%s\n", testKey, testValue))
	assert.Equal(t, int64(secondOffset), storage.index[testKey2], "")
}

func TestIndexedFileStorageIndexBuild(t *testing.T) {
	testutils.SetUp()
	defer testutils.Teardown()

	filename := ".test/db.mdb"
	storage := &Storage{
		Filename: filename,
	}
	storage.Start()

	// save some values to have initial data in the DB
	testKey := "t_key"
	testValue := "t_value"
	testKey2 := "t_key_2"
	testValue2 := "t_value_2"
	storage.Set(testKey, testValue)
	storage.Set(testKey2, testValue2)

	// clean the index and check it
	storage.index = map[string]int64{}
	assert.Equal(t, int64(0), storage.index[testKey], "index must be empty")
	assert.Equal(t, int64(0), storage.index[testKey2], "index must be empty")

	// build the index again
	storage.Stop()
	storage.Start()
	assert.Equal(t, int64(0), storage.index[testKey], "wrong index offset")
	secondOffset := len(fmt.Sprintf("%s;%s\n", testKey, testValue))
	assert.Equal(t, int64(secondOffset), storage.index[testKey2], "wrong index offset")
}

func TestIndexedFileStorageWhenThereIsNoKey(t *testing.T) {
	testutils.SetUp()
	defer testutils.Teardown()

	filename := ".test/db.mdb"
	storage := &Storage{
		Filename: filename,
		index:    map[string]int64{},
	}
	storage.Start()

	assert.Equal(t, 0, len(storage.index), "Index must be empty")

	value, exists := storage.Get("somekey")
	assert.Equal(t, "", value, "Wrong value")
	assert.False(t, exists)
}
