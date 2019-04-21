package block

import (
	"testing"

	"github.com/merlin82/leveldb/internal"
)

func Test_SsTable(t *testing.T) {
	var builder BlockBuilder

	item := internal.NewInternalKey(1, internal.TypeValue, []byte("123"), []byte("1234"))
	builder.Add(item)
	item = internal.NewInternalKey(2, internal.TypeValue, []byte("124"), []byte("1245"))
	builder.Add(item)
	item = internal.NewInternalKey(3, internal.TypeValue, []byte("125"), []byte("0245"))
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
