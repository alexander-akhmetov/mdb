package lsmt

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alexander-akhmetov/mdb/pkg/test_utils"
)

func TestCompactionWithoutFiles(t *testing.T) {
	// test compact() with only one file
	// it should not do anything
	testutils.SetUp()
	defer testutils.Teardown()

	// since we have only one file - there is nothing to merge
	testutils.CreateFile(".test/lsmt_data/sstables/0.sstable", "")

	f, s, c, isMerged := compact(
		".test/lsmt_data/sstables/",
		".test/lsmt_data/sstables/tmp/",
		2,
		defaultMaxCompactFileSize,
	)

	assert.False(t, isMerged)
	assert.Equal(t, "", f)
	assert.Equal(t, "", s)
	assert.Equal(t, "", c)
}

func TestSimpleCompaction(t *testing.T) {
	// test compaction with two files:
	// it should compact them into one
	testutils.SetUp()
	defer testutils.Teardown()

	testutils.CreateFileWithKeyValues(
		".test/lsmt_data/sstables/0.sstable",
		[][2]string{
			{"k1", "v1"},
			{"k2", "v2"},
		},
	)
	testutils.CreateFileWithKeyValues(
		".test/lsmt_data/sstables/1.sstable",
		[][2]string{
			{"k1", "v11"},
		},
	)
	testutils.CreateFileWithKeyValues(".test/lsmt_data/sstables/2.sstable", [][2]string{})

	f, s, c, isMerged := compact(
		".test/lsmt_data/sstables/",
		".test/lsmt_data/sstables/tmp/",
		2,
		defaultMaxCompactFileSize,
	)

	assert.True(t, isMerged)
	assert.Equal(t, ".test/lsmt_data/sstables/0.sstable", f)
	assert.Equal(t, ".test/lsmt_data/sstables/1.sstable", s)
	assert.Equal(t, ".test/lsmt_data/sstables/tmp/1.sstable", c)

	expData := [][2]string{
		{"k1", "v11"},
		{"k2", "v2"},
	}
	testutils.AssertKeysInFile(t, ".test/lsmt_data/sstables/tmp/1.sstable", expData)
}

func TestSimpleCompactionWithSameKeys(t *testing.T) {
	// test compaction process with same keys in different files:
	// it should save only the latest key-value pairs.
	testutils.SetUp()
	defer testutils.Teardown()

	testutils.CreateFileWithKeyValues(
		".test/lsmt_data/sstables/0.sstable",
		[][2]string{
			{"k1", "01"},
			{"k2", "02"},
		},
	)
	testutils.CreateFileWithKeyValues(
		".test/lsmt_data/sstables/1.sstable",
		[][2]string{
			{"k1", "11"},
			{"k2", "22"},
		},
	)

	f, s, c, isMerged := compact(
		".test/lsmt_data/sstables/",
		".test/lsmt_data/sstables/tmp/",
		2,
		defaultMaxCompactFileSize,
	)

	assert.True(t, isMerged)
	assert.Equal(t, ".test/lsmt_data/sstables/0.sstable", f)
	assert.Equal(t, ".test/lsmt_data/sstables/1.sstable", s)
	assert.Equal(t, ".test/lsmt_data/sstables/tmp/1.sstable", c)

	expData := [][2]string{
		{"k1", "11"},
		{"k2", "22"},
	}
	testutils.AssertKeysInFile(t, ".test/lsmt_data/sstables/tmp/1.sstable", expData)
}

func TestComplexCompaction(t *testing.T) {
	// test complex compaction situation:
	// many files, many keys with duplicates
	testutils.SetUp()
	defer testutils.Teardown()

	testutils.CreateFileWithKeyValues(
		".test/lsmt_data/sstables/0.sstable",
		[][2]string{
			{"k1", "1"},
			{"k2", "2"},
			{"k3", "3"},
			{"k3", "33"},
		},
	)
	testutils.CreateFileWithKeyValues(
		".test/lsmt_data/sstables/1.sstable",
		[][2]string{
			{"k1", "11"},
			{"k3", "333"},
			{"k5", "5"},
			{"k6", "6"},
		},
	)
	testutils.CreateFileWithKeyValues(".test/lsmt_data/sstables/2.sstable", [][2]string{})

	compact(".test/lsmt_data/sstables/", ".test/lsmt_data/sstables/tmp/", 2, defaultMaxCompactFileSize)

	expData := [][2]string{
		{"k1", "11"},
		{"k2", "2"},
		{"k3", "333"},
		{"k5", "5"},
		{"k6", "6"},
	}
	testutils.AssertKeysInFile(t, ".test/lsmt_data/sstables/tmp/1.sstable", expData)
}

func TestCompactionWithOneEmptyFile(t *testing.T) {
	// test compaction process with one empty file
	testutils.SetUp()
	defer testutils.Teardown()

	testutils.CreateFileWithKeyValues(".test/lsmt_data/sstables/0.sstable", [][2]string{})
	assert.True(t, testutils.IsFileExists(".test/lsmt_data/sstables/0.sstable"))

	secondFileKeys := [][2]string{
		{"k1", "11"},
		{"k3", "333"},
		{"k5", "5"},
		{"k6", "6"},
	}
	testutils.CreateFileWithKeyValues(".test/lsmt_data/sstables/1.sstable", secondFileKeys)
	testutils.CreateFileWithKeyValues(".test/lsmt_data/sstables/2.sstable", [][2]string{})
	assert.True(t, testutils.IsFileExists(".test/lsmt_data/sstables/2.sstable"))

	compact(".test/lsmt_data/sstables/", ".test/lsmt_data/sstables/tmp/", 2, defaultMaxCompactFileSize)

	testutils.AssertKeysInFile(t, ".test/lsmt_data/sstables/tmp/0.sstable", [][2]string{})
	testutils.AssertKeysInFile(t, ".test/lsmt_data/sstables/tmp/1.sstable", secondFileKeys)
	testutils.AssertKeysInFile(t, ".test/lsmt_data/sstables/tmp/2.sstable", [][2]string{})
}

func TestCompactionWithEmptySecondFile(t *testing.T) {
	testutils.SetUp()
	defer testutils.Teardown()

	firstFileKeys := [][2]string{
		{"k1", "1"},
		{"k3", "3"},
	}
	testutils.CreateFileWithKeyValues(".test/lsmt_data/sstables/0.sstable", firstFileKeys)
	testutils.CreateFileWithKeyValues(".test/lsmt_data/sstables/1.sstable", [][2]string{})
	testutils.CreateFileWithKeyValues(".test/lsmt_data/sstables/2.sstable", [][2]string{})

	compact(".test/lsmt_data/sstables/", ".test/lsmt_data/sstables/tmp/", 2, defaultMaxCompactFileSize)

	testutils.AssertKeysInFile(t, ".test/lsmt_data/sstables/tmp/1.sstable", firstFileKeys)
}

func TestCompactionWithEmptyFiles(t *testing.T) {
	// compaction with empty files should do nothing
	testutils.SetUp()
	defer testutils.Teardown()

	testutils.CreateFileWithKeyValues(".test/lsmt_data/sstables/0.sstable", [][2]string{})
	testutils.CreateFileWithKeyValues(".test/lsmt_data/sstables/1.sstable", [][2]string{})
	testutils.CreateFileWithKeyValues(".test/lsmt_data/sstables/2.sstable", [][2]string{})

	compact(".test/lsmt_data/sstables/", ".test/lsmt_data/sstables/tmp/", 2, defaultMaxCompactFileSize)

	testutils.AssertKeysInFile(t, ".test/lsmt_data/sstables/tmp/2.sstable", [][2]string{})
}
