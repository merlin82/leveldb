package sstable

import (
	"bytes"
	"encoding/gob"
)

type BlockBuilder struct {
	buf bytes.Buffer
	enc *gob.Encoder
}

func newBlockBuilder() *BlockBuilder {
	var block BlockBuilder
	block.enc = gob.NewEncoder(&block.buf)
	return &block
}

func (block *BlockBuilder) reset() {
	block.buf.Reset()
}

func (block *BlockBuilder) add(e interface{}) {
	block.enc.Encode(e)
}
func (block *BlockBuilder) finish() []byte {
	return block.buf.Bytes()
}

func (block *BlockBuilder) currentSizeEstimate() int {
	return block.buf.Len()
}

func (block *BlockBuilder) empty() bool {
	return block.buf.Len() == 0
}
