package block

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/merlin82/leveldb/format"
)

const (
	kTableMagicNumber uint64 = 0xdb4775248b80fb57
)

type BlockHandle struct {
	Offset uint32
	Size   uint32
}

func (blockHandle *BlockHandle) EncodeToBytes() []byte {
	p := make([]byte, 8)
	binary.LittleEndian.PutUint32(p, blockHandle.Offset)
	binary.LittleEndian.PutUint32(p[4:], blockHandle.Size)
	return p
}

func (blockHandle *BlockHandle) DecodeFromBytes(p []byte) {
	if len(p) == 8 {
		blockHandle.Offset = binary.LittleEndian.Uint32(p)
		blockHandle.Size = binary.LittleEndian.Uint32(p[4:])
	}
}

type IndexBlockHandle struct {
	LastKey *format.InternalKey
	BlockHandle
}

func (index *IndexBlockHandle) EncodeToInternalKey() *format.InternalKey {
	index.LastKey.UserValue = index.BlockHandle.EncodeToBytes()
	return index.LastKey
}

func (index *IndexBlockHandle) EncodeTo(w io.Writer) error {
	index.LastKey.EncodeTo(w)
	return binary.Write(w, binary.LittleEndian, index.BlockHandle)
}

func (index *IndexBlockHandle) DecodeFrom(r io.Reader) error {
	index.LastKey.DecodeFrom(r)
	return binary.Read(r, binary.LittleEndian, &index.BlockHandle)
}

type Footer struct {
	MetaIndexHandle BlockHandle
	IndexHandle     BlockHandle
}

func (footer *Footer) Size() int {
	// add magic size
	return binary.Size(footer) + 8
}

func (footer *Footer) EncodeTo(w io.Writer) error {
	err := binary.Write(w, binary.LittleEndian, footer)
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.LittleEndian, kTableMagicNumber)
	return err
}

func (footer *Footer) DecodeFrom(r io.Reader) error {
	err := binary.Read(r, binary.LittleEndian, footer)
	if err != nil {
		return err
	}
	var magic uint64
	err = binary.Read(r, binary.LittleEndian, &magic)
	if err != nil {
		return err
	}
	if magic != kTableMagicNumber {
		return errors.New("not an sstable (bad magic number)")
	}
	return nil
}
