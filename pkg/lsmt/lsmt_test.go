package lsmt

import (
	"testing"
	"time"

	"github.com/alexander-akhmetov/mdb/pkg/lsmt/internal/entry"
	"github.com/alexander-akhmetov/mdb/pkg/test_utils"
	"github.com/alexander-akhmetov/mdb/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestStorage(t *testing.T) {
	testutils.SetUp()
	defer testutils.Teardown()

	storage := &Storage{
		Config: StorageConfig{
			WorkDir:           ".test/lsmt",
			CompactionEnabled: false,
		},
	}
	storage.Start()
	defer storage.Stop()

	content := testutils.ReadFile(storage.memtable.logFilename)
	assert.Equal(t, "", content, "File content wrong")

	testKey := "key-lsmt"
	testValue := "value-lsmt"
	storage.Set(testKey, testValue)

	expData := [][2]string{
		{testKey, testValue},
	}
	testutils.AssertKeysInFile(t, storage.memtable.logFilename, expData)

	value, exists := storage.Get(testKey)
	assert.True(t, exists)
	assert.Equal(t, testValue, value)

	assert.Equal(t, testValue, storage.memtable.data[testKey])
}

func TestStorageSSTable(t *testing.T) {
	// we will create one sstable and check that it will be used correctly
	testutils.SetUp()
	defer testutils.Teardown()

	key1 := "k1"
	key2 := "k2"
	value1 := "v1"
	value2 := "v2"

	// let's create one simple sstable
	testutils.CreateFileWithKeyValues(
		".test/lsmt_data/sstables/0.sstable",
		[][2]string{
			{key1, value1},
			{key2, value2},
		},
	)

	storage := &Storage{
		Config: StorageConfig{
			WorkDir: ".test/lsmt_data/",
		},
	}
	storage.Start()
	defer storage.Stop()

	value, exists := storage.Get(key1)
	assert.True(t, exists)
	assert.Equal(t, value, value1)

	value, exists = storage.Get(key2)
	assert.True(t, exists)
	assert.Equal(t, value, value2)
}

func TestStorageSSTablesOrdering(t *testing.T) {
	// we will create many sstables and check that they are used correctly
	testutils.SetUp()
	defer testutils.Teardown()

	key1 := "k1"
	key2 := "k2"
	value1 := "v1"
	oldValue1 := "0"
	value2 := "v2"
	oldValue2 := "1"

	// let's create two sstables and check that we use them in the correct order
	testutils.CreateFileWithKeyValues(
		".test/lsmt_data/sstables/0.sstable",
		[][2]string{
			{key1, oldValue1},
			{key2, oldValue2},
		},
	)

	testutils.CreateFileWithKeyValues(
		".test/lsmt_data/sstables/1.sstable",
		[][2]string{
			{key1, value1},
			{key2, value2},
		},
	)

	storage := &Storage{
		Config: StorageConfig{
			WorkDir: ".test/lsmt_data/",
		},
	}
	storage.Start()
	defer storage.Stop()

	value, exists := storage.Get(key1)
	assert.True(t, exists)
	assert.Equal(t, value, value1)

	value, exists = storage.Get(key2)
	assert.True(t, exists)
	assert.Equal(t, value, value2)
}

func TestStorageAOLogRestoring(t *testing.T) {
	// we will create aolog and check that it will be restored to the db instance
	testutils.SetUp()
	defer testutils.Teardown()

	key1 := "k1"
	key2 := "k2"
	value1 := "v1"
	value2 := "v2"

	// let's create aolog, it should be the latest version of our keys
	testutils.CreateFileWithKeyValues(
		".test/lsmt_data/log.aolog",
		[][2]string{
			{key1, value1},
			{key2, value2},
		},
	)

	testutils.CreateFileWithKeyValues(
		".test/lsmt_data/sstables/1544288836377002.sstable",
		[][2]string{
			{"k1", "0"},
			{"k2", "0"},
		},
	)

	storage := &Storage{
		Config: StorageConfig{
			WorkDir: ".test/lsmt_data/",
		},
	}
	storage.Start()
	defer storage.Stop()

	value, exists := storage.Get(key1)
	assert.True(t, exists)
	assert.Equal(t, value1, value)

	value, exists = storage.Get(key2)
	assert.True(t, exists)
	assert.Equal(t, value2, value)
}

func TestStorageMemtablesToFlush(t *testing.T) {
	// we will create memtables to flush and check that they will be flushed and data will be used correctly
	testutils.SetUp()
	defer testutils.Teardown()

	key1 := "k1"
	key2 := "k2"
	value1 := "v1"
	oldValue1 := "0"
	value2 := "v2"
	oldValue2 := "1"

	// let's create aolog_tf, it should be the latest version of our keys
	data := [][2]string{
		{key1, value1},
		{key2, value2},
	}
	testutils.CreateFileWithKeyValues(".test/lsmt_data/aolog_tf/1.aolog", data)

	// sstable file will have older values
	data = [][2]string{
		{key1, oldValue1},
		{key2, oldValue2},
	}
	testutils.CreateFileWithKeyValues(".test/lsmt_data/sstables/0.sstable", data)

	storage := &Storage{
		Config: StorageConfig{
			WorkDir: ".test/lsmt_data/",
		},
	}
	storage.Start()
	defer storage.Stop()

	// wait for flush process
	time.Sleep(time.Millisecond * 200)

	value, exists := storage.Get(key1)
	assert.True(t, exists)
	assert.Equal(t, value1, value)

	value, exists = storage.Get(key2)
	assert.True(t, exists)
	assert.Equal(t, value2, value)

	expectedNewSSTablePath := ".test/lsmt_data/sstables/1.sstable"
	assert.True(t, testutils.IsFileExists(expectedNewSSTablePath))

	// manually clean memtables and memtablesToFlush queue to check that data will be readed from SSTable
	storage.memtablesFlushQueue = []*memtable{}
	storage.memtable.data = map[string]string{}

	value, exists = storage.Get(key1)
	assert.True(t, exists)
	assert.Equal(t, value1, value)

	value, exists = storage.Get(key2)
	assert.True(t, exists)
	assert.Equal(t, value2, value)
}

func TestStorageMemtableMustBeMoreImportantThanMemtablesToFlush(t *testing.T) {
	// we will aolog and aolog_tf (to flush) to check that data will be readed only from memtable, because it's
	// more important truth source
	testutils.SetUp()
	defer testutils.Teardown()

	key1 := "k1"
	key2 := "k2"
	value1 := "v1"
	oldValue1 := "0"
	value2 := "v2"
	oldValue2 := "1"

	// let's create aolog_tf, it should be the latest version of our keys
	data := [][2]string{
		{key1, value1},
		{key2, value2},
	}
	testutils.CreateFileWithKeyValues(".test/lsmt_data/log.aolog", data)

	data = [][2]string{
		{key1, oldValue1},
		{key2, oldValue2},
	}
	testutils.CreateFileWithKeyValues(".test/lsmt_data/aolog_tf/0.aolog", data)

	storage := &Storage{
		Config: StorageConfig{
			WorkDir: ".test/lsmt_data/",
		},
	}
	storage.Start()
	defer storage.Stop()

	// wait for flush process
	time.Sleep(time.Millisecond * 200)

	value, exists := storage.Get(key1)
	assert.True(t, exists)
	assert.Equal(t, value1, value)

	value, exists = storage.Get(key2)
	assert.True(t, exists)
	assert.Equal(t, value2, value)
}

func TestStorageMemtablesToFlushOnly(t *testing.T) {
	// we will create aolog_tf only and check that we still read data
	testutils.SetUp()
	defer testutils.Teardown()

	key1 := "k1"
	key2 := "k2"
	value1 := "v1"
	value2 := "v2"

	testutils.CreateFileWithKeyValues(
		".test/lsmt_data/aolog_tf/0.aolog",
		[][2]string{
			{key1, value1},
			{key2, value2},
		},
	)

	storage := &Storage{
		Config: StorageConfig{
			WorkDir: ".test/lsmt_data/",
		},
	}
	// lock flush process
	flushMutex.Lock()
	storage.Start()
	defer storage.Stop()

	value, exists := storage.Get(key1)
	assert.True(t, exists)
	assert.Equal(t, value1, value)

	value, exists = storage.Get(key2)
	assert.True(t, exists)
	assert.Equal(t, value2, value)

	assert.True(t, testutils.IsDirEmpty(".test/lsmt_data/sstables/"))
	assert.Equal(t, 0, len(storage.ssTables))

	// unlock flush process
	flushMutex.Unlock()
	time.Sleep(time.Millisecond * 200)

	// now we should have one sstable
	assert.False(t, testutils.IsDirEmpty(storage.Config.ssTablesDir))
	assert.Equal(t, 1, len(storage.ssTables))
	// and no memtables to flush
	assert.Equal(t, 0, len(storage.memtablesFlushQueue))
	assert.True(t, testutils.IsDirEmpty(storage.Config.memtablesFlushTmpDir))

	// and we still have these keys and values :)
	value, exists = storage.Get(key1)
	assert.True(t, exists)
	assert.Equal(t, value1, value)

	value, exists = storage.Get(key2)
	assert.True(t, exists)
	assert.Equal(t, value2, value)
}

func TestStorageWithManyMemtablesToFlush(t *testing.T) {
	// we will create many memtables to flush and check that we use them in correct order
	testutils.SetUp()
	defer testutils.Teardown()

	key1 := "k1"
	key2 := "k2"
	value1 := "v1"
	oldValue1 := "oldv1"
	value2 := "v2"
	oldValue2 := "oldv2"

	file1 := ".test/lsmt_data/aolog_tf/0.aolog"
	utils.CreateFileIfNotExists(file1)
	appendBinaryToFile(file1, &entry.DBEntry{
		Type:  0,
		Key:   key1,
		Value: oldValue1,
	})
	appendBinaryToFile(file1, &entry.DBEntry{
		Type:  0,
		Key:   key2,
		Value: oldValue2,
	})

	file2 := ".test/lsmt_data/aolog_tf/1.aolog"
	utils.CreateFileIfNotExists(file2)
	appendBinaryToFile(file2, &entry.DBEntry{
		Type:  0,
		Key:   key1,
		Value: value1,
	})
	appendBinaryToFile(file2, &entry.DBEntry{
		Type:  0,
		Key:   key2,
		Value: value2,
	})

	storage := &Storage{
		Config: StorageConfig{
			WorkDir: ".test/lsmt_data/",
		},
	}
	// lock flush process
	flushMutex.Lock()
	storage.Start()
	defer storage.Stop()

	value, exists := storage.Get(key1)
	assert.True(t, exists)
	assert.Equal(t, value1, value)

	value, exists = storage.Get(key2)
	assert.True(t, exists)
	assert.Equal(t, value2, value)

	assert.True(t, testutils.IsDirEmpty(".test/lsmt_data/sstables/"))
	assert.Equal(t, 0, len(storage.ssTables))

	// unlock flush process
	flushMutex.Unlock()
	time.Sleep(time.Millisecond * 200)

	// now we should have one sstable
	assert.False(t, testutils.IsDirEmpty(storage.Config.ssTablesDir))
	assert.Equal(t, 2, len(storage.ssTables))
	// and no memtables to flush
	assert.Equal(t, 0, len(storage.memtablesFlushQueue))
	assert.True(t, testutils.IsDirEmpty(storage.Config.memtablesFlushTmpDir))

	// and we still have these keys and values :)
	value, exists = storage.Get(key1)
	assert.True(t, exists)
	assert.Equal(t, value1, value)

	value, exists = storage.Get(key2)
	assert.True(t, exists)
	assert.Equal(t, value2, value)
}

func TestStorageCompaction(t *testing.T) {
	// we will create SSTables and wait for compaction process
	testutils.SetUp()
	defer testutils.Teardown()

	key1 := "k1"
	key2 := "k2"
	value1 := "11"
	oldValue1 := "1"
	value2 := "22"
	oldValue2 := "2"

	testutils.CreateFileWithKeyValues(
		".test/lsmt_data/sstables/0.sstable",
		[][2]string{
			{key1, oldValue1},
			{key2, oldValue2},
		},
	)

	testutils.CreateFileWithKeyValues(
		".test/lsmt_data/sstables/1.sstable",
		[][2]string{
			{key1, value1},
			{key2, value2},
		},
	)

	storage := &Storage{
		Config: StorageConfig{
			WorkDir:           ".test/lsmt_data/",
			CompactionEnabled: true,
		},
	}
	storage.Start()
	defer storage.Stop()

	// wait for compaction process
	time.Sleep(time.Millisecond * 200)

	value, exists := storage.Get(key1)
	assert.True(t, exists)
	assert.Equal(t, value1, value)

	value, exists = storage.Get(key2)
	assert.True(t, exists)
	assert.Equal(t, value2, value)

	expectedNewSSTablePath := ".test/lsmt_data/sstables/1.sstable"
	assert.True(t, testutils.IsFileExists(expectedNewSSTablePath))
	assert.False(t, testutils.IsFileExists(".test/lsmt_data/sstables/0.sstable"))

	binaryContent := testutils.ReadFileBinary(expectedNewSSTablePath)
	expContent := []byte{0x0, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x2, 0x6b, 0x31, 0x31, 0x31, 0x0, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x2, 0x6b, 0x32, 0x32, 0x32}
	assert.Equal(t, expContent, binaryContent)
}

func TestStorageCompactionForOldFiles(t *testing.T) {
	// we will create SSTables and wait for compaction process
	testutils.SetUp()
	defer testutils.Teardown()

	key1 := "k1"
	key2 := "k2"
	value1 := "111"
	oldValue1 := "11"
	oldestValue1 := "1"
	value2 := "222"
	oldestValue2 := "22"
	oldValue2 := "2"

	testutils.CreateFileWithKeyValues(
		".test/lsmt_data/sstables/0.sstable",
		[][2]string{
			{key1, oldestValue1},
			{key2, oldestValue2},
		},
	)

	testutils.CreateFileWithKeyValues(
		".test/lsmt_data/sstables/1.sstable",
		[][2]string{
			{key1, oldValue1},
			{key2, oldValue2},
		},
	)

	testutils.CreateFileWithKeyValues(
		".test/lsmt_data/sstables/2.sstable",
		[][2]string{
			{key1, value1},
			{key2, value2},
		},
	)

	storage := &Storage{
		Config: StorageConfig{
			WorkDir:               ".test/lsmt_data/",
			CompactionEnabled:     true,
			MinimumFilesToCompact: 3,
		},
	}
	storage.Start()
	defer storage.Stop()

	// wait for compaction process
	time.Sleep(time.Millisecond * 200)

	value, exists := storage.Get(key1)
	assert.True(t, exists)
	assert.Equal(t, value1, value)

	value, exists = storage.Get(key2)
	assert.True(t, exists)
	assert.Equal(t, value2, value)

	expectedNewSSTablePath := ".test/lsmt_data/sstables/1.sstable"
	assert.True(t, testutils.IsFileExists(expectedNewSSTablePath))
	assert.False(t, testutils.IsFileExists(".test/lsmt_data/sstables/0.sstable"))

	expData := [][2]string{
		{key1, oldValue1},
		{key2, oldValue2},
	}
	testutils.AssertKeysInFile(t, expectedNewSSTablePath, expData)
}
