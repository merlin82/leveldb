package version

import (
	"fmt"
	"testing"

	"github.com/merlin82/leveldb/internal"
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
