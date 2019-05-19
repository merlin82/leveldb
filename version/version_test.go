package version

import (
	"fmt"
	"testing"

	"github.com/merlin82/leveldb/internal"
	"github.com/merlin82/leveldb/memtable"
)

func Test_Version_Get(t *testing.T) {
	v := New("D:\\")
	var f FileMetaData
	f.number = 123
	f.smallest = internal.NewInternalKey(1, internal.TypeValue, []byte("123"), nil)
	f.largest = internal.NewInternalKey(1, internal.TypeValue, []byte("125"), nil)
	v.files[0] = append(v.files[0], &f)

	value, err := v.Get([]byte("125"))
	fmt.Println(err, value)
}

func Test_Version_Load(t *testing.T) {
	v := New("D:\\leveldbtest")
	memTable := memtable.New()
	memTable.Add(1234567, internal.TypeValue, []byte("aadsa34a"), []byte("bb23b3423"))
	v.WriteLevel0Table(memTable)
	n, _ := v.Save()
	fmt.Println(v)

	v2, _ := Load("D:\\leveldbtest", n)
	fmt.Println(v2)
	value, err := v2.Get([]byte("aadsa34a"))
	fmt.Println(err, value)
}
