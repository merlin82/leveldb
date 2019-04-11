package sstable

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	kTableMagicNumber uint64 = 0xdb4775248b80fb57
)

type BlockHandle struct {
	Offset int32
	Size   int32
}

type IndexBlockHandle struct {
	LastKey []byte
	BlockHandle
}

func (index *IndexBlockHandle) EncodeTo(w io.Writer) error {
	binary.Write(w, binary.LittleEndian, int32(len(index.LastKey)))
	binary.Write(w, binary.LittleEndian, index.LastKey)
	return binary.Write(w, binary.LittleEndian, index.BlockHandle)
}

func (index *IndexBlockHandle) DecodeFrom(r io.Reader) error {
	var tmp int32
	index.LastKey = make([]byte, tmp)
	binary.Read(r, binary.LittleEndian, index.LastKey)
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
