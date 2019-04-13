package sstable

import (
	"github.com/merlin82/leveldb/format"
	"github.com/merlin82/leveldb/sstable/block"
)

type Iterator struct {
	table           *SsTable
	dataBlockHandle BlockHandle
	dataIter        *block.Iterator
	indexIter       *block.Iterator
}

// Returns true iff the iterator is positioned at a valid node.
func (it *Iterator) Valid() bool {
	return it.dataIter != nil && it.dataIter.Valid()
}

func (it *Iterator) InternalKey() *format.InternalKey {
	return it.dataIter.InternalKey()
}

// Advances to the next position.
// REQUIRES: Valid()
func (it *Iterator) Next() {
	it.dataIter.Next()
	it.skipEmptyDataBlocksForward()
}

// Advances to the previous position.
// REQUIRES: Valid()
func (it *Iterator) Prev() {
	it.dataIter.Prev()
	it.skipEmptyDataBlocksBackward()
}

// Advance to the first entry with a key >= target
func (it *Iterator) Seek(target interface{}) {
	// Index Block的block_data字段中，每一条记录的key都满足：
	// 大于等于Data Block的所有key，并且小于后面所有Data Block的key
	// 因为Seek是查找key>=target的第一条记录，所以当index_iter_找到时，
	// 该index_inter_对应的data_iter_所管理的Data Block中所有记录的
	// key都小于等于target，如果需要在下一个Data Block中seek，而下一个Data Block
	// 中的第一条记录就满足key>=target

	it.indexIter.Seek(target)
	it.initDataBlock()
	if it.dataIter != nil {

		it.dataIter.Seek(target)
	}
	it.skipEmptyDataBlocksForward()
}

// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *Iterator) SeekToFirst() {
	it.indexIter.SeekToFirst()
	it.initDataBlock()
	if it.dataIter != nil {
		it.dataIter.SeekToFirst()
	}
	it.skipEmptyDataBlocksForward()
}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *Iterator) SeekToLast() {
	it.indexIter.SeekToLast()
	it.initDataBlock()
	if it.dataIter != nil {
		it.dataIter.SeekToLast()
	}
	it.skipEmptyDataBlocksBackward()
}

func (it *Iterator) initDataBlock() {
	if !it.indexIter.Valid() {
		it.dataIter = nil
	} else {
		var index IndexBlockHandle
		index.InternalKey = it.indexIter.InternalKey()
		tmpBlockHandle := index.GetBlockHandle()

		if it.dataIter != nil && it.dataBlockHandle == tmpBlockHandle {
			// data_iter_ is already constructed with this iterator, so
			// no need to change anything
		} else {
			it.dataIter = it.table.readBlock(tmpBlockHandle).NewIterator()
			it.dataBlockHandle = tmpBlockHandle
		}
	}
}

func (it *Iterator) skipEmptyDataBlocksForward() {
	for it.dataIter == nil || !it.dataIter.Valid() {
		if !it.indexIter.Valid() {
			it.dataIter = nil
			return
		}
		it.indexIter.Next()
		it.initDataBlock()
		if it.dataIter != nil {
			it.dataIter.SeekToFirst()
		}
	}
}

func (it *Iterator) skipEmptyDataBlocksBackward() {
	for it.dataIter == nil || !it.dataIter.Valid() {
		if !it.indexIter.Valid() {
			it.dataIter = nil
			return
		}
		it.indexIter.Prev()
		it.initDataBlock()
		if it.dataIter != nil {
			it.dataIter.SeekToLast()
		}
	}
}
