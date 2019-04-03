package memtable

import (
	"fmt"
	"testing"
)

func Test_MemTable(t *testing.T) {
	memTable := New()
	memTable.Add(1234567, TypeValue, []byte("aadsa34a"), []byte("bb23b3423"))
	_, value, _ := memTable.Get([]byte("aadsa34a"))
	fmt.Println(string(value))

}
