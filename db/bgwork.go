package db

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/merlin82/leveldb/internal"
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
	version := db.current.Copy()
	db.mu.Unlock()
	version.WriteLevel0Table(db.imm)
	descriptorNumber, _ := version.Save()
	db.SetCurrentFile(descriptorNumber)
	db.mu.Lock()
	db.imm = nil
	db.current = version
}

func (db *Db) SetCurrentFile(descriptorNumber uint64) {
	tmp := internal.TempFileName(db.name, descriptorNumber)
	ioutil.WriteFile(tmp, []byte(fmt.Sprintf("%d", descriptorNumber)), 0600)
	os.Rename(tmp, internal.CurrentFileName(db.name))
}
