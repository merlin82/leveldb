package version

import (
	"log"
	"os"
	"sort"

	"github.com/merlin82/leveldb/internal"
)

type FileMetaData struct {
	allowSeeks uint64
	number     uint64
	fileSize   uint64
	smallest   *internal.InternalKey
	largest    *internal.InternalKey
}

type Version struct {
	tableCache     *TableCache
	nextFileNumber uint64
	seq            uint64
	files          [internal.NumLevels][]*FileMetaData
	// Per-level key at which the next compaction at that level should start.
	// Either an empty string, or a valid InternalKey.
	compactPointer [internal.NumLevels]*internal.InternalKey
}

func New(dbName string) *Version {
	var v Version
	v.tableCache = NewTableCache(dbName)
	v.nextFileNumber = 1
	return &v
}

func Load(dbName string, number uint64) (*Version, error) {
	fileName := internal.DescriptorFileName(dbName, number)
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	v := New(dbName)
	return v, v.DecodeFrom(file)
}

func (v *Version) Save() (uint64, error) {
	tmp := v.nextFileNumber
	fileName := internal.DescriptorFileName(v.tableCache.dbName, v.nextFileNumber)
	v.nextFileNumber++
	file, err := os.Create(fileName)
	if err != nil {
		return tmp, err
	}
	defer file.Close()
	return tmp, v.EncodeTo(file)
}
func (v *Version) Log() {
	for level := 0; level < internal.NumLevels; level++ {
		for i := 0; i < len(v.files[level]); i++ {
			log.Printf("version[%d]: %d", level, v.files[level][i].number)
		}
	}
}
func (v *Version) Copy() *Version {
	var c Version

	c.tableCache = v.tableCache
	c.nextFileNumber = v.nextFileNumber
	c.seq = v.seq
	for level := 0; level < internal.NumLevels; level++ {
		c.files[level] = make([]*FileMetaData, len(v.files[level]))
		copy(c.files[level], v.files[level])
	}
	return &c
}
func (v *Version) NextSeq() uint64 {
	v.seq++
	return v.seq
}

func (v *Version) NumLevelFiles(l int) int {
	return len(v.files[l])
}

func (v *Version) Get(key []byte) ([]byte, error) {
	var tmp []*FileMetaData
	var tmp2 [1]*FileMetaData
	var files []*FileMetaData
	// We can search level-by-level since entries never hop across
	// levels.  Therefore we are guaranteed that if we find data
	// in an smaller level, later levels are irrelevant.
	for level := 0; level < internal.NumLevels; level++ {
		numFiles := len(v.files[level])
		if numFiles == 0 {
			continue
		}
		if level == 0 {
			// Level-0 files may overlap each other.  Find all files that
			// overlap user_key and process them in order from newest to oldest.
			for i := 0; i < numFiles; i++ {
				f := v.files[level][i]
				if internal.UserKeyComparator(key, f.smallest.UserKey) >= 0 && internal.UserKeyComparator(key, f.largest.UserKey) <= 0 {
					tmp = append(tmp, f)
				}
			}
			if len(tmp) == 0 {
				continue
			}
			sort.Slice(tmp, func(i, j int) bool { return tmp[i].number > tmp[j].number })
			numFiles = len(tmp)
			files = tmp
		} else {
			index := v.findFile(v.files[level], key)
			if index >= numFiles {
				files = nil
				numFiles = 0
			} else {
				tmp2[0] = v.files[level][index]
				if internal.UserKeyComparator(key, tmp2[0].smallest.UserKey) < 0 {
					files = nil
					numFiles = 0
				} else {
					files = tmp2[:]
					numFiles = 1
				}
			}
		}
		for i := 0; i < numFiles; i++ {
			f := files[i]
			value, err := v.tableCache.Get(f.number, key)
			if err != internal.ErrNotFound {
				return value, err
			}
		}
	}
	return nil, internal.ErrNotFound
}

func (v *Version) findFile(files []*FileMetaData, key []byte) int {
	left := 0
	right := len(files)
	for left < right {
		mid := (left + right) / 2
		f := files[mid]
		if internal.UserKeyComparator(f.largest.UserKey, key) < 0 {
			// Key at "mid.largest" is < "target".  Therefore all
			// files at or before "mid" are uninteresting.
			left = mid + 1
		} else {
			// Key at "mid.largest" is >= "target".  Therefore all files
			// after "mid" are uninteresting.
			right = mid
		}
	}
	return right
}
