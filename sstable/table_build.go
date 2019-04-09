package sstable

import (
	"bytes"
	"encoding/gob"
	"io"

	"github.com/merlin82/leveldb/format"
)

const (
	MAX_BLOCK_SIZE = 4 * 1024
)

type TableBuilder struct {
	writer             io.Writer
	offset             int
	numEntries         int
	dataBlock          *BlockBuilder
	indexBlock         *BlockBuilder
	pendingIndexEntry  bool
	pendingIndexHandle IndexBlockHandle
	status             error
}

func NewTableBuilder(writer io.Writer) *TableBuilder {
	var builder TableBuilder
	builder.writer = writer
	builder.pendingIndexEntry = false
	builder.dataBlock = newBlockBuilder()
	builder.indexBlock = newBlockBuilder()
	return &builder
}

func (builder *TableBuilder) Add(internalKey *format.InternalKey) {
	if builder.status != nil {
		return
	}
	if builder.pendingIndexEntry {
		builder.indexBlock.add(builder.pendingIndexHandle)
		builder.pendingIndexEntry = false
	}
	// todo : filter block

	builder.pendingIndexHandle.LastKey = internalKey.UserKey
	builder.numEntries++
	builder.dataBlock.add(internalKey)
	if builder.dataBlock.currentSizeEstimate() > MAX_BLOCK_SIZE {
		builder.flush()
	}
}
func (builder *TableBuilder) flush() {
	if builder.dataBlock.empty() {
		return
	}
	builder.pendingIndexHandle.BlockHandle = builder.writeblock(builder.dataBlock)
	builder.pendingIndexEntry = true
}

func (builder *TableBuilder) Finish() error {
	// write data block
	builder.flush()
	// todo : filter block

	// write index block
	if builder.pendingIndexEntry {
		builder.indexBlock.add(builder.pendingIndexHandle)
		builder.pendingIndexEntry = false
	}
	var footer Footer
	footer.IndexHandle = builder.writeblock(builder.indexBlock)

	// write footer block, 40 byte
	footerRaw := make([]byte, 40)
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(footer)
	copy(footerRaw, buf.Bytes())
	// todo : magic
	builder.writer.Write(footerRaw)
	return nil
}

func (builder *TableBuilder) writeblock(block *BlockBuilder) BlockHandle {
	content := block.finish()
	// todo : compress, crc
	builder.writer.Write(content)
	var blockHandle BlockHandle
	blockHandle.Offset = builder.offset
	blockHandle.Size = len(content)
	builder.offset += len(content)
	_, builder.status = builder.writer.Write(content)

	block.reset()
	return blockHandle
}
