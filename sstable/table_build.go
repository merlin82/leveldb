package sstable

import (
	"os"

	"github.com/merlin82/leveldb/format"
	"github.com/merlin82/leveldb/sstable/block"
)

const (
	MAX_BLOCK_SIZE = 4 * 1024
)

type TableBuilder struct {
	file               *os.File
	offset             uint32
	numEntries         int32
	dataBlockBuilder   block.BlockBuilder
	indexBlockBuilder  block.BlockBuilder
	pendingIndexEntry  bool
	pendingIndexHandle IndexBlockHandle
	status             error
}

func NewTableBuilder(fileName string) *TableBuilder {
	var builder TableBuilder
	var err error
	builder.file, err = os.Create(fileName)
	if err != nil {
		return nil
	}
	builder.pendingIndexEntry = false
	return &builder
}

func (builder *TableBuilder) Add(internalKey *format.InternalKey) {
	if builder.status != nil {
		return
	}
	if builder.pendingIndexEntry {
		builder.indexBlockBuilder.Add(builder.pendingIndexHandle.InternalKey)
		builder.pendingIndexEntry = false
	}
	// todo : filter block

	builder.pendingIndexHandle.InternalKey = internalKey

	builder.numEntries++
	builder.dataBlockBuilder.Add(internalKey)
	if builder.dataBlockBuilder.CurrentSizeEstimate() > MAX_BLOCK_SIZE {
		builder.flush()
	}
}
func (builder *TableBuilder) flush() {
	if builder.dataBlockBuilder.Empty() {
		return
	}
	builder.pendingIndexHandle.SetBlockHandle(builder.writeblock(&builder.dataBlockBuilder))
	builder.pendingIndexEntry = true
}

func (builder *TableBuilder) Finish() error {
	// write data block
	builder.flush()
	// todo : filter block

	// write index block
	if builder.pendingIndexEntry {
		builder.indexBlockBuilder.Add(builder.pendingIndexHandle.InternalKey)
		builder.pendingIndexEntry = false
	}
	var footer Footer
	footer.IndexHandle = builder.writeblock(&builder.indexBlockBuilder)

	// write footer block
	footer.EncodeTo(builder.file)
	builder.file.Close()
	return nil
}

func (builder *TableBuilder) writeblock(blockBuilder *block.BlockBuilder) BlockHandle {
	content := blockBuilder.Finish()
	// todo : compress, crc
	var blockHandle BlockHandle
	blockHandle.Offset = builder.offset
	blockHandle.Size = uint32(len(content))
	builder.offset += uint32(len(content))
	_, builder.status = builder.file.Write(content)
	builder.file.Sync()
	blockBuilder.Reset()
	return blockHandle
}
