package db

import (
	"fmt"
	"os"

	"github.com/merlin82/leveldb/memtable"
	"github.com/merlin82/leveldb/sstable"
)

func makeFileName(name string, number int, suffix string) string {
	return fmt.Sprintf("%s/%06d.%s", name, number, suffix)
}

func tableFileName(name string, number int) string {
	return makeFileName(name, number, "ldb")
}

func buildTable(dbname string, table *memtable.MemTable) error {
	file, err := os.Create(tableFileName(dbname, 123))
	if err != nil {
		return err
	}
	builder := sstable.NewTableBuilder(file)
	iter := table.NewIterator()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		builder.Add(iter.InternalKey())
	}
	builder.Finish()
	file.Close()
	return nil
}
