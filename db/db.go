package db

import (
	"sync"
	"sync/atomic"

	"time"

	"github.com/merlin82/leveldb/internal"
	"github.com/merlin82/leveldb/memtable"
	"github.com/merlin82/leveldb/version"
)

type Db struct {
	name                  string
	mu                    sync.Mutex
	cond                  *sync.Cond
	seq                   int64
	mem                   *memtable.MemTable
	imm                   *memtable.MemTable
	current               *version.Version
	bgCompactionScheduled bool
}

func Open(dbName string) *Db {
	var db Db
	db.name = dbName
	db.seq = 0
	db.mem = memtable.New()
	db.imm = nil
	db.cond = sync.NewCond(&db.mu)
	db.current = version.New(dbName)
	db.bgCompactionScheduled = false
	return &db
}

func (db *Db) Close() {
	db.mu.Lock()
	for db.bgCompactionScheduled {
		db.cond.Wait()
	}
	db.mu.Unlock()
}

func (db *Db) Put(key, value []byte) error {
	// May temporarily unlock and wait.
	err := db.makeRoomForWrite()
	if err != nil {
		return err
	}
	seq := atomic.AddInt64(&db.seq, 1) //version

	// todo : add log

	db.mem.Add(seq, internal.TypeValue, key, value)
	return nil
}

func (db *Db) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	mem := db.mem
	imm := db.mem
	current := db.current
	db.mu.Unlock()
	value, err := mem.Get(key)
	if err != internal.ErrNotFound {
		return value, err
	}

	if imm != nil {
		value, err := imm.Get(key)
		if err != internal.ErrNotFound {
			return value, err
		}
	}

	value, err = current.Get(key)
	return value, err
}

func (db *Db) Delete(key []byte) error {
	seq := atomic.AddInt64(&db.seq, 1)
	db.mem.Add(seq, internal.TypeDeletion, key, nil)
	return nil
}

func (db *Db) makeRoomForWrite() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	for true {
		if db.current.NumLevelFiles(0) >= internal.L0_SlowdownWritesTrigger {
			db.mu.Unlock()
			time.Sleep(time.Duration(1000) * time.Microsecond)
			db.mu.Lock()
		} else if db.mem.ApproximateMemoryUsage() <= internal.Write_buffer_size {
			return nil
		} else if db.imm != nil {
			//  Current memtable full; waiting
			db.cond.Wait()
		} else {
			// Attempt to switch to a new memtable and trigger compaction of old
			// todo : switch log
			db.imm = db.mem
			db.mem = memtable.New()
			db.maybeScheduleCompaction()
		}
	}

	return nil
}
