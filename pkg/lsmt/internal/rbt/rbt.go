package rbt

import (
	rbt "github.com/emirpasic/gods/trees/redblacktree"
)

// RedBlackTree data structure
type RedBlackTree struct {
	*rbt.Tree
}

// NewRBTree returns a new Red-Black Tree
func NewRBTree() *RedBlackTree {
	return &RedBlackTree{rbt.NewWithStringComparator()}
}

// GetClosest returns value of by key or the closest minimal one
func (tree *RedBlackTree) GetClosest(value string) int {
	return tree.getClosestValue(tree.Root, value, -1)
}

func (tree *RedBlackTree) getClosestValue(node *rbt.Node, key string, currentClosest int) int {
	if node == nil {
		return currentClosest
	}

	nodeKey := node.Key.(string)
	nodeValue := node.Value.(int)

	if currentClosest == -1 {
		currentClosest = nodeValue
	}

	// reverse order
	if nodeKey > key {
		return tree.getClosestValue(node.Left, key, currentClosest)
	}

	if nodeKey == key {
		return nodeValue
	}

	if nodeKey < key {
		return tree.getClosestValue(node.Right, key, nodeValue)
	}

	return -1
}
