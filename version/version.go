package version

import (
	"sort"

	"github.com/merlin82/leveldb/internal"
)

type FileMetaData struct {
	allowSeeks int
	number     uint64
	fileSize   uint64
	smallest   internal.InternalKey
	largest    internal.InternalKey
}

type Version struct {
	tableCache *TableCache
	files      [internal.NumLevels][]*FileMetaData
}

func New(dbName string) *Version {
	var v Version
	v.tableCache = newTableCache(dbName)
	return &v
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
				if internal.InternalKeyComparator(key, f.smallest) >= 0 && internal.InternalKeyComparator(key, f.largest) <= 0 {
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
				if internal.InternalKeyComparator(key, tmp2[0].smallest) < 0 {
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
			found, value, err := v.tableCache.Get(f.number, key)
			if found {
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
		if internal.InternalKeyComparator(f.largest, key) < 0 {
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
