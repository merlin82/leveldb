package version

import (
	"encoding/binary"
	"io"

	"github.com/merlin82/leveldb/internal"
	"github.com/merlin82/leveldb/memtable"
	"github.com/merlin82/leveldb/sstable"
)

func (meta *FileMetaData) EncodeTo(w io.Writer) error {
	binary.Write(w, binary.LittleEndian, meta.allowSeeks)
	binary.Write(w, binary.LittleEndian, meta.fileSize)
	binary.Write(w, binary.LittleEndian, meta.number)
	meta.smallest.EncodeTo(w)
	meta.largest.EncodeTo(w)
	return nil
}

func (meta *FileMetaData) DecodeFrom(r io.Reader) error {
	binary.Read(r, binary.LittleEndian, meta.allowSeeks)
	binary.Read(r, binary.LittleEndian, meta.fileSize)
	binary.Read(r, binary.LittleEndian, meta.number)
	meta.smallest = new(internal.InternalKey)
	meta.smallest.DecodeFrom(r)
	meta.largest = new(internal.InternalKey)
	meta.largest.DecodeFrom(r)
	return nil
}

func (v *Version) EncodeTo(w io.Writer) error {
	binary.Write(w, binary.LittleEndian, v.nextFileNumber)
	for level := 0; level < internal.NumLevels; level++ {
		numFiles := len(v.files[level])
		binary.Write(w, binary.LittleEndian, numFiles)
		for i := 0; i < numFiles; i++ {
			v.files[level][i].EncodeTo(w)
		}
	}
	return nil
}

func (v *Version) DecodeFrom(r io.Reader) error {

	var numFiles int32
	binary.Read(r, binary.LittleEndian, &v.nextFileNumber)
	for level := 0; level < internal.NumLevels; level++ {
		binary.Read(r, binary.LittleEndian, &numFiles)
		v.files[level] = make([]*FileMetaData, numFiles)
		for i := 0; i < int(numFiles); i++ {
			v.files[level][i].DecodeFrom(r)
		}
	}
	return nil
}

func (v *Version) addFile(level int, meta *FileMetaData) {
	if level == 0 {
		// 0层没有排序
		v.files[level] = append(v.files[level], meta)
	}
}

func (v *Version) WriteLevel0Table(imm *memtable.MemTable) {
	var meta FileMetaData
	meta.allowSeeks = 1 << 30
	meta.number = v.nextFileNumber
	v.nextFileNumber++

	builder := sstable.NewTableBuilder((internal.TableFileName(v.tableCache.dbName, meta.number)))
	iter := imm.NewIterator()
	iter.SeekToFirst()
	if iter.Valid() {
		meta.smallest = iter.InternalKey()
		for ; iter.Valid(); iter.Next() {
			meta.largest = iter.InternalKey()
			builder.Add(iter.InternalKey())
		}
		builder.Finish()
		meta.fileSize = uint64(builder.FileSize())
	}
	// todo 挑选合适的level
	v.addFile(0, &meta)
}

func (v *Version) DoCompactionWork() {
	// for level
	// pick
	// 多路归并
}
