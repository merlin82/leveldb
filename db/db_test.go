package db

import (
	"fmt"
	"testing"
)

func Test_Db(t *testing.T) {
	db := Open()
	db.Put([]byte("123"), []byte("456"))

	value, err := db.Get([]byte("123"))
	fmt.Println(string(value))

	db.Delete([]byte("123"))
	value, err = db.Get([]byte("123"))
	fmt.Println(err)

	db.Put([]byte("123"), []byte("789"))
	value, _ = db.Get([]byte("123"))
	fmt.Println(string(value))
}
