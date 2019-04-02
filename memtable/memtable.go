package memtable

import (
	"errors"

	"github.com/merlin82/leveldb/skiplist"
)

type MemTable struct {
	table *skiplist.SkipList
}

func New() *MemTable {
	var memTable MemTable
	memTable.table = skiplist.New(InternalKeyComparator)
	return &memTable
}
func (memTable *MemTable) Add(seq SequenceNumber, valueType ValueType, key, value []byte) {
	internalKey := newInternalKey(seq, valueType, key, value)
	memTable.table.Insert(internalKey)
}

func (memTable *MemTable) Get(key []byte) (bool, []byte, error) {
	lookupKey := LookupKey(key)

	it := memTable.table.NewIterator()
	it.Seek(lookupKey)
	if it.Valid() {
		internalKey := it.Key().(*InternalKey)
		if UserKeyComparator(key, internalKey.userKey()) == 0 {
			// 判断valueType
			if internalKey.valueType() == kTypeValue {
				return true, internalKey.userValue(), nil
			} else {
				return true, nil, errors.New("not found")
			}
		}
	}
	return false, nil, errors.New("not found")
}
