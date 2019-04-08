package leveldb

import (
	"github.com/merlin82/leveldb/db"
)

type LevelDb interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
}

type Iterator interface {
	// Returns true iff the iterator is positioned at a valid node.
	Valid() bool

	// Returns the key at the current position.
	// REQUIRES: Valid()
	Key() interface{}

	// Advances to the next position.
	// REQUIRES: Valid()
	Next()

	// Advances to the previous position.
	// REQUIRES: Valid()
	Prev()

	// Advance to the first entry with a key >= target
	Seek(target interface{})

	// Position at the first entry in list.
	// Final state of iterator is Valid() iff list is not empty.
	SeekToFirst()

	// Position at the last entry in list.
	// Final state of iterator is Valid() iff list is not empty.
	SeekToLast()
}

func Open() LevelDb {
	return db.Open()
}
