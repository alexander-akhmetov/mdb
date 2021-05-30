package file

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alexander-akhmetov/mdb/pkg/test_utils"
)

func TestFileStorage(t *testing.T) {
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

	value, exists := storage.Get(testKey2)
	assert.Equal(t, testValue2, value, "Wrong value")
	assert.True(t, exists)

	value, exists = storage.Get(testKey)
	assert.Equal(t, testValue, value, "Wrong value")
	assert.True(t, exists)
}
