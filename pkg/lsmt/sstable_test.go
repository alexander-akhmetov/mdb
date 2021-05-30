package lsmt

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/alexander-akhmetov/mdb/pkg/lsmt/internal/entry"
	"github.com/alexander-akhmetov/mdb/pkg/utils"

	"github.com/stretchr/testify/assert"

	"github.com/alexander-akhmetov/mdb/pkg/test_utils"
)

func TestListSSTables(t *testing.T) {
	// test that listSSTables returns the list of paths to the files with ss tables
	// and they are ordered by modification time
	testutils.SetUp()
	defer testutils.Teardown()

	sstablesDir := "./.test/sstables-test/"
	os.MkdirAll(sstablesDir, os.ModePerm)

	assert.Equal(t, []utils.FileInfo{}, listSSTables(sstablesDir))

	files := []string{"file.sstable", "another.sstable", "sometmpfile.txt"}

	for _, f := range files {
		os.OpenFile(filepath.Join(sstablesDir, f), os.O_RDONLY|os.O_CREATE, 0600)
	}

	expFiles := []utils.FileInfo{
		{
			Name: ".test/sstables-test/another.sstable",
			Size: 0,
		},
		{
			Name: ".test/sstables-test/file.sstable",
			Size: 0,
		},
	}
	assert.Equal(t, expFiles, listSSTables(sstablesDir))
}

func TestSSTableGet(t *testing.T) {
	// test that SSTable reads content from the file

	// create two key-value pairs manually and read them with .Get method
	testutils.SetUp()
	defer testutils.Teardown()

	sstablesDir := "./.test/sstables-test/"
	os.MkdirAll(sstablesDir, os.ModePerm)

	filePath := filepath.Join(sstablesDir, "TestSSTableGet")

	key1 := "key1"
	value1 := "value1"
	key2 := "key2"
	value2 := "value2"

	f, _ := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0600)
	f.Close()

	appendBinaryToFile(filePath, &entry.DBEntry{
		Type:  0,
		Key:   key1,
		Value: value1,
	})
	appendBinaryToFile(filePath, &entry.DBEntry{
		Type:  0,
		Key:   key2,
		Value: value2,
	})

	ssTable := newSSTable(&ssTableConfig{filename: filePath})

	value, exists := ssTable.Get(key1)
	assert.True(t, exists)
	assert.Equal(t, value1, value)

	value, exists = ssTable.Get(key2)
	assert.True(t, exists)
	assert.Equal(t, value2, value)

	value, exists = ssTable.Get("unknownkey")
	assert.False(t, exists)
	assert.Equal(t, "", value)
}

func TestRebuildSparseIndexWithOneElement(t *testing.T) {
	testutils.SetUp()
	defer testutils.Teardown()

	sstablesDir := "./.test/sstables-test/"
	os.MkdirAll(sstablesDir, os.ModePerm)

	filePath := filepath.Join(sstablesDir, "ftest.sstable")
	os.OpenFile(filePath, os.O_RDONLY|os.O_CREATE, 0600)

	e := &entry.DBEntry{
		Type:  0,
		Key:   "key",
		Value: "value",
	}
	appendBinaryToFile(filePath, e)

	ssTable := newSSTable(&ssTableConfig{filename: filePath})

	ssTable.rebuildSparseIndex()

	v, f := ssTable.index.Get("key")
	assert.Equal(t, 0, v)
	assert.True(t, f)
}

func TestRebuildSparseIndexWithManyElements(t *testing.T) {
	testutils.SetUp()
	defer testutils.Teardown()

	sstablesDir := "./.test/sstables-test/"
	os.MkdirAll(sstablesDir, os.ModePerm)

	filePath := filepath.Join(sstablesDir, "ftest.sstable")
	os.OpenFile(filePath, os.O_RDONLY|os.O_CREATE, 0600)

	for _, i := range []int{1, 2, 3, 4, 5} {
		appendBinaryToFile(filePath, &entry.DBEntry{
			Type:  0,
			Key:   fmt.Sprintf("key_%v", i),
			Value: fmt.Sprintf("value_%v", i),
		})
	}

	ssTable := newSSTable(
		&ssTableConfig{
			filename:       filePath,
			readBufferSize: 80, // key_5 should start at 84
		},
	)

	ssTable.rebuildSparseIndex()

	v, f := ssTable.index.Get("key_1")
	assert.Equal(t, 0, v)
	assert.True(t, f)

	v, f = ssTable.index.Get("key_5")
	assert.Equal(t, 84, v)
	assert.True(t, f)

	v, e := ssTable.Get("key_5")
	assert.True(t, e)
	assert.Equal(t, "value_5", v)
}

func TestRebuildSparseIndexWithManyElementsAndIncompleteLast(t *testing.T) {
	testutils.SetUp()
	defer testutils.Teardown()

	sstablesDir := "./.test/sstables-test/"
	os.MkdirAll(sstablesDir, os.ModePerm)

	filePath := filepath.Join(sstablesDir, "ftest.sstable")
	os.OpenFile(filePath, os.O_RDONLY|os.O_CREATE, 0600)

	for _, i := range []int{1, 2, 3, 4, 5} {
		appendBinaryToFile(filePath, &entry.DBEntry{
			Type:  0,
			Key:   fmt.Sprintf("key_%v", i),
			Value: fmt.Sprintf("value_%v", i),
		})
	}
	file, _ := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0600)
	file.Write([]byte{0})
	file.Close()

	ssTable := newSSTable(
		&ssTableConfig{
			filename:       filePath,
			readBufferSize: 80, // key_5 should start at 84
		},
	)
	ssTable.rebuildSparseIndex()

	v, f := ssTable.index.Get("key_1")
	assert.Equal(t, 0, v)
	assert.True(t, f)

	v, f = ssTable.index.Get("key_5")
	assert.Equal(t, 84, v)
	assert.True(t, f)
}

func TestRebuildSparseIndexWithManyElementsAndSmallBufferSize(t *testing.T) {
	testutils.SetUp()
	defer testutils.Teardown()

	sstablesDir := "./.test/sstables-test/"
	os.MkdirAll(sstablesDir, os.ModePerm)

	filePath := filepath.Join(sstablesDir, "ftest.sstable")
	os.OpenFile(filePath, os.O_RDONLY|os.O_CREATE, 0600)

	for _, i := range []int{1, 2, 3, 4, 5} {
		appendBinaryToFile(filePath, &entry.DBEntry{
			Type:  0,
			Key:   fmt.Sprintf("key_%v", i),
			Value: fmt.Sprintf("value_%v", i),
		})
	}

	ssTable := newSSTable(
		&ssTableConfig{
			filename:       filePath,
			readBufferSize: 1,
		},
	)

	keys := map[string]int{
		"key_1": 0,
		"key_2": 21,
		"key_3": 42,
		"key_4": 63,
		"key_5": 84,
	}
	for key, offset := range keys {
		v, f := ssTable.index.Get(key)
		assert.Equal(t, offset, v)
		assert.True(t, f)
	}
}
