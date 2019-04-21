package version

import (
	"fmt"
	"sync"

	"github.com/hashicorp/golang-lru"
	"github.com/merlin82/leveldb/internal"
	"github.com/merlin82/leveldb/sstable"
)

type TableCache struct {
	mu     sync.Mutex
	dbName string
	cache  *lru.Cache
}

func newTableCache(dbName string) *TableCache {
	var tableCache TableCache
	tableCache.dbName = dbName
	tableCache.cache, _ = lru.New(internal.MaxOpenFiles - internal.NumNonTableCacheFiles)
	return &tableCache
}

func (tableCache *TableCache) NewIterator(fileNum uint64) *sstable.Iterator {
	table, _ := tableCache.findTable(fileNum)
	if table != nil {
		return table.NewIterator()
	}
	return nil
}
func (tableCache *TableCache) Get(fileNum uint64, key []byte) (bool, []byte, error) {
	table, err := tableCache.findTable(fileNum)
	if table != nil {
		return table.Get(key)
	}

	return false, nil, err
}

func (tableCache *TableCache) Evict(fileNum uint64) {
	tableCache.cache.Remove(fileNum)
}

func (tableCache *TableCache) findTable(fileNum uint64) (*sstable.SsTable, error) {
	tableCache.mu.Lock()
	defer tableCache.mu.Unlock()
	table, ok := tableCache.cache.Get(fileNum)
	if ok {
		return table.(*sstable.SsTable), nil
	} else {
		ssTable, err := sstable.Open(tableFileName(tableCache.dbName, fileNum))
		tableCache.cache.Add(fileNum, ssTable)
		return ssTable, err
	}
}

func makeFileName(name string, number uint64, suffix string) string {
	return fmt.Sprintf("%s/%06d.%s", name, number, suffix)
}

func tableFileName(name string, number uint64) string {
	return makeFileName(name, number, "ldb")
}
