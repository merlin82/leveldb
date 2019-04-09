package sstable

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"testing"
)

func Test_BlockBuilder(t *testing.T) {

	blockHandle := BlockHandle{Offset: 123, Size: 456}
	block := newBlockBuilder()
	block.add([]byte("1234"))
	block.add(blockHandle)
	buf := block.finish()
	fmt.Println(buf)

	r := bytes.NewBuffer(buf)
	decoder := gob.NewDecoder(r)

	var s []byte
	var b BlockHandle
	decoder.Decode(&s)
	err := decoder.Decode(&b)
	fmt.Println(err)
	fmt.Println(s)
	fmt.Println(b)
}
