package sstable

import (
	"bytes"
	//	"encoding/binary"
	"io"
)

type BlockItem interface {
	EncodeTo(w io.Writer) error
	DecodeFrom(r io.Reader) error
}
type BlockBuilder struct {
	buf bytes.Buffer
}

func (block *BlockBuilder) reset() {
	block.buf.Reset()
}

func (block *BlockBuilder) add(item BlockItem) error {
	return item.EncodeTo(&block.buf)
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
