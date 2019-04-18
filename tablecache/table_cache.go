package tablecache

import (
	"fmt"

	"github.com/merlin82/leveldb"
	"github.com/merlin82/leveldb/sstable"
)

var (
	DBName = ""
)

func NewIterator(fileNum uint64) leveldb.Iterator {
	table, _ := findTable(fileNum)
	if table != nil {
		return table.NewIterator()
	}
	return nil
}
func Get(fileNum uint64, key []byte) ([]byte, error) {
	table, err := findTable(fileNum)
	if table != nil {
		return table.Get(key)
	}

	return nil, err
}

func Evict(fileNum uint64) {

}

func findTable(fileNum uint64) (*sstable.SsTable, error) {
	table, err := sstable.Open(tableFileName(DBName, fileNum))
	return table, err
}

func makeFileName(name string, number uint64, suffix string) string {
	return fmt.Sprintf("%s/%06d.%s", name, number, suffix)
}

func tableFileName(name string, number uint64) string {
	return makeFileName(name, number, "ldb")
}
