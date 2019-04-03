package db

import (
	"sync/atomic"

	"github.com/merlin82/leveldb/memtable"
)

type Db struct {
	seq int64
	mem *memtable.MemTable
}

func Open() *Db {
	var db Db
	db.seq = 0
	db.mem = memtable.New()
	return &db
}

func (db *Db) Put(key, value []byte) error {
	seq := atomic.AddInt64(&db.seq, 1)
	db.mem.Add(seq, memtable.TypeValue, key, value)
	return nil
}
func (db *Db) Get(key []byte) ([]byte, error) {
	found, value, err := db.mem.Get(key)
	if !found {

	}
	return value, err
}
func (db *Db) Delete(key []byte) error {
	seq := atomic.AddInt64(&db.seq, 1)
	db.mem.Add(seq, memtable.TypeDeletion, key, nil)
	return nil
}
