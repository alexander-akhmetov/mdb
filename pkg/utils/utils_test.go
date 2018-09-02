package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alexander-akhmetov/mdb/pkg/test_utils"
)

func TestTrimKey(t *testing.T) {
	value := "some-value"
	key := "some-key"
	testStr := fmt.Sprintf("%s;%s", key, value)

	if TrimKey("some-key", testStr) != value {
		t.Errorf("trimKey must return value only")
	}
}

func TestListFilesOrdered(t *testing.T) {
	// test that ListFilesOrdered returns list of files with path
	// and they are ordered by their name
	testutils.SetUp()
	defer testutils.Teardown()

	filesDir := "./.test/list-files-test/"
	os.MkdirAll(filesDir, os.ModePerm)

	filterBySuffix := ".sstable"
	assert.Equal(t, []FileInfo{}, ListFilesOrdered(filesDir, filterBySuffix))

	files := []string{"file.sstable", "another.sstable", "sometmpfile.txt"}

	for _, f := range files {
		os.OpenFile(filepath.Join(filesDir, f), os.O_RDONLY|os.O_CREATE, 0600)
	}

	// expFiles with ordered by last modified time and with full path
	expFiles := []FileInfo{
		FileInfo{
			Name: ".test/list-files-test/another.sstable",
			Size: 0,
		},
		FileInfo{
			Name: ".test/list-files-test/file.sstable",
			Size: 0,
		},
	}
	assert.Equal(t, expFiles, ListFilesOrdered(filesDir, filterBySuffix))
}

func TestListFilesOrderedWithoutSuffix(t *testing.T) {
	// test that ListFilesOrdered returns list of files with path
	// and they are ordered by their name
	testutils.SetUp()
	defer testutils.Teardown()

	filesDir := "./.test/list-files-test/"
	os.MkdirAll(filesDir, os.ModePerm)

	assert.Equal(t, []FileInfo{}, ListFilesOrdered(filesDir, ""))

	files := []string{"3.sstable", "2.sstable", "1.txt"}

	for _, f := range files {
		os.OpenFile(filepath.Join(filesDir, f), os.O_RDONLY|os.O_CREATE, 0600)
	}

	// expFiles with ordered by last modified time and with full path
	expFiles := []FileInfo{
		FileInfo{
			Name: ".test/list-files-test/3.sstable",
			Size: 0,
		},
		FileInfo{
			Name: ".test/list-files-test/2.sstable",
			Size: 0,
		},
		FileInfo{
			Name: ".test/list-files-test/1.txt",
			Size: 0,
		},
	}
	assert.Equal(t, expFiles, ListFilesOrdered(filesDir, ""))
}

func TestGetKeyValueFromString(t *testing.T) {
	key, value := GetKeyValueFromString("key;value\n")
	assert.Equal(t, "key", key)
	assert.Equal(t, "value", value)

	key, value = GetKeyValueFromString("key;key;key;value")
	assert.Equal(t, "key", key)
	assert.Equal(t, "key;key;value", value)
}
