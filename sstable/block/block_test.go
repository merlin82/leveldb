package block

import (
	"fmt"
	"testing"

	"github.com/merlin82/leveldb/format"
)

func Test_SsTable(t *testing.T) {
	var builder BlockBuilder

	item := format.NewInternalKey(1, format.TypeValue, []byte("123"), []byte("1234"))
	builder.Add(item)
	item = format.NewInternalKey(2, format.TypeValue, []byte("124"), []byte("1245"))
	builder.Add(item)
	item = format.NewInternalKey(3, format.TypeValue, []byte("125"), []byte("0245"))
	builder.Add(item)
	p := builder.Finish()

	block := New(p)
	it := block.NewIterator()

	it.Seek([]byte("1244"))
	if it.Valid() {
		if string(it.InternalKey().UserKey) != "125" {
			t.Fail()
		}

	} else {
		t.Fail()
	}
}
