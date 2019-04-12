package block

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/merlin82/leveldb/format"
)

type Block struct {
	items []*format.InternalKey
}

func New(r io.ReaderAt, blockHandle BlockHandle) *Block {
	var block Block
	p := make([]byte, blockHandle.Size)
	n, err := r.ReadAt(p, int64(blockHandle.Offset))
	if err != nil || uint32(n) != blockHandle.Size {
		return nil
	}
	data := bytes.NewBuffer(p)
	counter := binary.LittleEndian.Uint32(p[len(p)-4:])
	for i := uint32(0); i < counter; i++ {
		var item *format.InternalKey
		binary.Read(data, binary.LittleEndian, item)
		block.items = append(block.items, item)
	}

	return &block
}

func (block *Block) NewIterator() *Iterator {
	return &Iterator{block: block}
}
