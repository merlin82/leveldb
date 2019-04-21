package memtable

import (
	"github.com/merlin82/leveldb/internal"
	"github.com/merlin82/leveldb/skiplist"
)

type Iterator struct {
	listIter *skiplist.Iterator
}

// Returns true iff the iterator is positioned at a valid node.
func (it *Iterator) Valid() bool {
	return it.listIter.Valid()
}

func (it *Iterator) InternalKey() *internal.InternalKey {
	return it.listIter.Key().(*internal.InternalKey)
}

// Advances to the next position.
// REQUIRES: Valid()
func (it *Iterator) Next() {
	it.listIter.Next()
}

// Advances to the previous position.
// REQUIRES: Valid()
func (it *Iterator) Prev() {
	it.listIter.Prev()
}

// Advance to the first entry with a key >= target
func (it *Iterator) Seek(target interface{}) {
	it.listIter.Seek(target)
}

// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *Iterator) SeekToFirst() {
	it.listIter.SeekToFirst()
}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *Iterator) SeekToLast() {
	it.listIter.SeekToLast()
}
