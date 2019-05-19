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
	binary.Read(r, binary.LittleEndian, &meta.allowSeeks)
	binary.Read(r, binary.LittleEndian, &meta.fileSize)
	binary.Read(r, binary.LittleEndian, &meta.number)
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
		binary.Write(w, binary.LittleEndian, int32(numFiles))

		for i := 0; i < numFiles; i++ {
			v.files[level][i].EncodeTo(w)
		}
	}
	return nil
}

func (v *Version) DecodeFrom(r io.Reader) error {

	binary.Read(r, binary.LittleEndian, &v.nextFileNumber)

	var numFiles int32
	for level := 0; level < internal.NumLevels; level++ {
		binary.Read(r, binary.LittleEndian, &numFiles)
		v.files[level] = make([]*FileMetaData, numFiles)
		for i := 0; i < int(numFiles); i++ {
			var meta FileMetaData
			meta.DecodeFrom(r)
			v.files[level][i] = &meta
		}
	}
	return nil
}

func (v *Version) addFile(level int, meta *FileMetaData) {
	if level == 0 {
		// 0层没有排序
		v.files[level] = append(v.files[level], meta)
	} else {
		v.files[level] = append(v.files[level], meta)
		numFiles := len(v.files[level])
		//todo: 二分法
		for i := 0; i < numFiles-1; i++ {
			if internal.InternalKeyComparator(v.files[level][i].largest, meta.smallest) < 0 {
				v.files[level][i], v.files[level][numFiles-1] = v.files[level][numFiles-1], v.files[level][i]
			}
		}
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
		meta.smallest.UserValue = nil
		meta.largest.UserValue = nil
	}

	// 挑选合适的level
	level := 0
	if !v.overlapInLevel(0, meta.smallest.UserKey, meta.largest.UserKey) {
		for ; level < internal.MaxMemCompactLevel; level++ {
			if v.overlapInLevel(level+1, meta.smallest.UserKey, meta.largest.UserKey) {
				break
			}
		}
	}

	v.addFile(level, &meta)
}

func (v *Version) overlapInLevel(level int, smallestKey, largestKey []byte) bool {
	numFiles := len(v.files[level])
	if numFiles == 0 {
		return false
	}
	if level == 0 {
		for i := 0; i < numFiles; i++ {
			f := v.files[level][i]
			if internal.UserKeyComparator(smallestKey, f.largest.UserKey) > 0 || internal.UserKeyComparator(f.smallest.UserKey, largestKey) > 0 {
				continue
			} else {
				return true
			}
		}
	} else {
		index := v.findFile(v.files[level], smallestKey)
		if index >= numFiles {
			return false
		}
		if internal.UserKeyComparator(largestKey, v.files[level][index].smallest.UserKey) > 0 {
			return true
		}
	}
	return false
}

func (v *Version) DoCompactionWork() {
	// for level
	// pick
	// 多路归并
}
