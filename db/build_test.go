package db

import (
	"fmt"
	"testing"
)

func Test_buildTable(t *testing.T) {
	db := Open()
	db.Put([]byte("123"), []byte("456"))

	err := buildTable("D:\\", db.mem)
	fmt.Println(err)
}
