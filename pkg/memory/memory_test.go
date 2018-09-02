package memory

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemoryTestSet(t *testing.T) {
	db := Storage{
		storage: map[string]string{},
	}

	testKey := "test-key"
	testValue := "test-value"
	db.Set(testKey, testValue)

	if db.storage[testKey] != testValue {
		t.Errorf("MemoryStorage Set error")
	}
}

func TestMemoryTestGet(t *testing.T) {
	db := Storage{
		storage: map[string]string{},
	}

	testKey := "test-key"
	testValue := "test-value"
	db.storage[testKey] = testValue

	value, exists := db.Get(testKey)
	assert.Equal(t, testValue, value, "Wrong value")
	assert.True(t, exists)
}

func TestMemoryStorageSetGet(t *testing.T) {
	db := Storage{
		storage: map[string]string{},
	}

	testKey := "test-key"
	testValue := "test-value"

	db.Set(testKey, testValue)

	value, exists := db.Get(testKey)
	assert.Equal(t, testValue, value, "Wrong value")
	assert.True(t, exists)
}

// func TestMemoryStorageDelete(t *testing.T) {
// 	storage := MemoryStorage{
// 		storage: map[string]string{},
// 	}

// 	testKey := "test-key"
// 	testValue := "test-value"

// 	storage.Set(testKey, testValue)

// 	value, exists := storage.Get(testKey)
// 	assert.Equal(t, testValue, value, "Wrong value")
// 	assert.True(t, exists)

// 	storage.Delete(testKey)
// 	value, exists = storage.Get(testKey)
// 	assert.Equal(t, "", value, "Wrong value")
// 	assert.False(t, exists)

// 	assert.Equal(t, 0, len(storage.storage), "Storage must be empty")
// }
