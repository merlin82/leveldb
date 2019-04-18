package db

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/merlin82/leveldb/config"
	"github.com/merlin82/leveldb/format"
	"github.com/merlin82/leveldb/memtable"
	"github.com/merlin82/leveldb/version"
)

type Db struct {
	mu      sync.Mutex
	seq     int64
	mem     *memtable.MemTable
	imm     *memtable.MemTable
	current *version.Version
}

func Open() *Db {
	var db Db
	db.seq = 0
	db.mem = memtable.New()
	db.imm = nil
	return &db
}

func (db *Db) Put(key, value []byte) error {
	// May temporarily unlock and wait.
	err := db.makeRoomForWrite()
	if err != nil {
		return err
	}
	seq := atomic.AddInt64(&db.seq, 1) //version

	// todo : add log

	//
	db.mem.Add(seq, format.TypeValue, key, value)
	return nil
}

func (db *Db) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	mem := db.mem
	imm := db.mem
	current := db.current
	db.mu.Unlock()
	found, value, err := mem.Get(key)
	if found {
		return value, err
	}

	if imm != nil {
		found, value, err := imm.Get(key)
		if found {
			return value, err
		}
	}

	value, err = current.Get(key)
	return value, err
}

func (db *Db) Delete(key []byte) error {
	seq := atomic.AddInt64(&db.seq, 1)
	db.mem.Add(seq, format.TypeDeletion, key, nil)
	return nil
}

func (db *Db) makeRoomForWrite() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for true {
		if db.current.NumLevelFiles(0) >= config.L0_SlowdownWritesTrigger {
			db.mu.Unlock()
			time.Sleep(time.Duration(1000) * time.Microsecond)
			db.mu.Lock()
		} else if db.mem.ApproximateMemoryUsage() <= config.Write_buffer_size {
			return nil
		} else if db.imm != nil {
			//  Current memtable full; waiting
			// todo:condition
		} else {
			// Attempt to switch to a new memtable and trigger compaction of old
			// todo : switch log
			db.imm = db.mem
			db.mem = memtable.New()
			// todo : MaybeScheduleCompaction
		}
	}

	return nil
}
