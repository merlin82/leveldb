package skiplist

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/merlin82/leveldb/utils"
)

func Test_Insert(t *testing.T) {
	skiplist := New(utils.IntComparator)
	for i := 0; i < 100; i++ {
		skiplist.Insert(rand.Int() % 100)
	}
	it := skiplist.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		fmt.Println(it.Key())
	}
	fmt.Println()
	for it.SeekToLast(); it.Valid(); it.Prev() {
		fmt.Println(it.Key())
	}

}
