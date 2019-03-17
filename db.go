// leveldb project leveldb.go
package leveldb

type DB struct {
}

func Open(path string) (*DB, error) {
	db := new(DB)
	return db, nil
}

func Close(db *DB) error {
	return nil
}

func (db *DB) Get(key Slice) error {
	return nil
}

func (db *DB) Put(key, value Slice) error {
	return nil
}

func (db *DB) Delete(key Slice) error {
	return nil
}
