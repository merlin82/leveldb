package sstable

import (
	"fmt"

	"testing"
)

func Test_SsTable(t *testing.T) {
	table, err := Open("D:\\000123.ldb")
	fmt.Println(err)
	if err == nil {
		fmt.Println(table.index)
		fmt.Println(table.footer)
	}
}
