package memtable

import (
	"fmt"
	"testing"

	"github.com/merlin82/leveldb/format"
)

func Test_MemTable(t *testing.T) {
	memTable := New()
	memTable.Add(1234567, format.TypeValue, []byte("aadsa34a"), []byte("bb23b3423"))
	_, value, _ := memTable.Get([]byte("aadsa34a"))
	fmt.Println(string(value))
	fmt.Println(memTable.ApproximateMemoryUsage())
}
