package sstable

import (
	"errors"

	"io"
	"os"

	"github.com/merlin82/leveldb/sstable/block"
)

type SsTable struct {
	index  *block.Block
	footer Footer
	file   *os.File
}

func Open(fileName string) (*SsTable, error) {
	var table SsTable
	var err error
	table.file, err = os.Open(fileName)
	if err != nil {
		return nil, err
	}
	stat, _ := table.file.Stat()
	// Read the footer block
	footerSize := int64(table.footer.Size())
	if stat.Size() < footerSize {
		return nil, errors.New("file is too short to be an sstable")
	}

	_, err = table.file.Seek(-footerSize, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	err = table.footer.DecodeFrom(table.file)
	if err != nil {
		return nil, err
	}
	// Read the index block
	table.index = table.readBlock(table.footer.IndexHandle)
	return &table, nil
}

func (table *SsTable) NewIterator() *Iterator {
	var it Iterator
	it.table = table
	it.indexIter = table.index.NewIterator()
	return &it
}

func Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (table *SsTable) readBlock(blockHandle BlockHandle) *block.Block {
	p := make([]byte, blockHandle.Size)
	n, err := table.file.ReadAt(p, int64(blockHandle.Offset))
	if err != nil || uint32(n) != blockHandle.Size {
		return nil
	}

	return block.New(p)
}
