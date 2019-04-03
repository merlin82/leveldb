package memtable

import (
	"fmt"
	"testing"
)

func Test_InternalKey(t *testing.T) {
	internalKey := newInternalKey(1234567, TypeValue, []byte("aadsa34a"), []byte("bb23b3423"))
	fmt.Println(string(internalKey.userKey()))
	fmt.Println(string(internalKey.userValue()))
	fmt.Println(internalKey.valueType())
	fmt.Println(internalKey.seq())
}
