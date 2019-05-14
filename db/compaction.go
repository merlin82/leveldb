package db

import (
	"github.com/merlin82/leveldb/version"
)

func (db *Db) maybeScheduleCompaction() {
	if db.bgCompactionScheduled {
		return
	}
	db.bgCompactionScheduled = true
	go db.backgroundCall()
}

func (db *Db) backgroundCall() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.backgroundCompaction()
	db.bgCompactionScheduled = false

	// Previous compaction may have produced too many files in a level,
	// so reschedule another compaction if needed.
	db.maybeScheduleCompaction()
	db.cond.Broadcast()
}

func (db *Db) backgroundCompaction() {
	if db.imm != nil {
		db.compactMemTable()
		return
	}
	// new version
}

func (db *Db) compactMemTable() {
	//var meta version.FileMetaData
	//meta.number = db.current.NewFileNumber()

}
