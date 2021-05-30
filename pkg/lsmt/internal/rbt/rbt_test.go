package rbt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindClosestValueInRBTree(t *testing.T) {
	// GetClosest() should return the value for the given key
	// or for the closest key (min).
	tree := NewRBTree()

	tree.Put("key_a", 0)
	tree.Put("key_g", 5)
	tree.Put("key_p", 10)

	assert.Equal(t, 0, tree.GetClosest("key_a"))
	assert.Equal(t, 0, tree.GetClosest("key_b"))

	assert.Equal(t, 5, tree.GetClosest("key_g"))
	assert.Equal(t, 5, tree.GetClosest("key_i"))
	assert.Equal(t, 5, tree.GetClosest("key_j"))

	assert.Equal(t, 10, tree.GetClosest("key_p"))
	assert.Equal(t, 10, tree.GetClosest("key_q"))
	assert.Equal(t, 10, tree.GetClosest("key_s"))
	assert.Equal(t, 10, tree.GetClosest("key_z"))
}
