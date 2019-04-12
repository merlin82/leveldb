package sstable

import (
	"errors"
	"io"
	"os"

	"github.com/merlin82/leveldb/sstable/block"
)

type SsTable struct {
	index  *block.Block
	footer block.Footer
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
	table.index = block.New(table.file, table.footer.IndexHandle)
	return &table, nil
}

func NewIterator() {

}

func Get(key []byte) ([]byte, error) {
	return nil, nil
}
