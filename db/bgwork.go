package db

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"

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
	db.cond.Broadcast()
}

func (db *Db) backgroundCompaction() {
	imm := db.imm
	version := db.current.Copy()
	db.mu.Unlock()

	// minor compaction
	if imm != nil {
		version.WriteLevel0Table(imm)
	}
	// major compaction
	for version.DoCompactionWork() {
		version.Log()
	}
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

func (db *Db) ReadCurrentFile() uint64 {
	b, err := ioutil.ReadFile(internal.CurrentFileName(db.name))
	if err != nil {
		return 0
	}
	descriptorNumber, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return 0
	}
	return descriptorNumber
}
