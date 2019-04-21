package sstable

import (
	"fmt"

	"testing"

	"github.com/merlin82/leveldb/internal"
)

func Test_SsTable(t *testing.T) {
	builder := NewTableBuilder("D:\\000123.ldb")
	item := internal.NewInternalKey(1, internal.TypeValue, []byte("123"), []byte("1234"))
	builder.Add(item)
	item = internal.NewInternalKey(2, internal.TypeValue, []byte("124"), []byte("1245"))
	builder.Add(item)
	item = internal.NewInternalKey(3, internal.TypeValue, []byte("125"), []byte("0245"))
	builder.Add(item)
	builder.Finish()

	table, err := Open("D:\\000123.ldb")
	fmt.Println(err)
	if err == nil {
		fmt.Println(table.index)
		fmt.Println(table.footer)
	}
	it := table.NewIterator()
	it.Seek([]byte("1244"))
	if it.Valid() {
		if string(it.InternalKey().UserKey) != "125" {
			t.Fail()
		}
	} else {
		t.Fail()
	}
}
