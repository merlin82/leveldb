package memtable

import (
	"errors"

	"github.com/merlin82/leveldb/format"
	"github.com/merlin82/leveldb/skiplist"
)

type MemTable struct {
	table       *skiplist.SkipList
	memoryUsage uint64
}

func New() *MemTable {
	var memTable MemTable
	memTable.table = skiplist.New(format.InternalKeyComparator)
	return &memTable
}

func (memTable *MemTable) NewIterator() *Iterator {
	return &Iterator{listIter: memTable.table.NewIterator()}
}

func (memTable *MemTable) Add(seq int64, valueType format.ValueType, key, value []byte) {
	internalKey := format.NewInternalKey(seq, valueType, key, value)

	memTable.memoryUsage += uint64(16 + len(key) + len(value))
	memTable.table.Insert(internalKey)
}

func (memTable *MemTable) Get(key []byte) (bool, []byte, error) {
	lookupKey := format.LookupKey(key)

	it := memTable.table.NewIterator()
	it.Seek(lookupKey)
	if it.Valid() {
		internalKey := it.Key().(*format.InternalKey)
		if format.UserKeyComparator(key, internalKey.UserKey) == 0 {
			// 判断valueType
			if internalKey.Type == format.TypeValue {
				return true, internalKey.UserValue, nil
			} else {
				return true, nil, errors.New("not found")
			}
		}
	}
	return false, nil, errors.New("not found")
}

func (memTable *MemTable) ApproximateMemoryUsage() uint64 {
	return memTable.memoryUsage
}
