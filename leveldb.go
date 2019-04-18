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
	Key() []byte

	// Return the value for the current entry.  The underlying storage for
	// the returned slice is valid only until the next modification of
	// the iterator.
	// REQUIRES: Valid()
	Value() []byte

	// Advances to the next position.
	// REQUIRES: Valid()
	Next()

	// Advances to the previous position.
	// REQUIRES: Valid()
	Prev()

	// Advance to the first entry with a key >= target
	Seek(target []byte)

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
