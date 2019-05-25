package db

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

//生成随机字符串
func GetRandomString(lenth int) []byte {
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bytes := []byte(str)
	result := []byte{}

	for i := 0; i < lenth; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return result
}

func Test_Db(t *testing.T) {
	db := Open("D:\\leveldbtest")
	db.Put([]byte("123"), []byte("456"))

	value, err := db.Get([]byte("123"))
	fmt.Println(string(value))

	db.Delete([]byte("123"))
	value, err = db.Get([]byte("123"))
	fmt.Println(err)

	db.Put([]byte("123"), []byte("789"))
	value, _ = db.Get([]byte("123"))
	fmt.Println(string(value))
	db.Close()
}

func Test_Db2(t *testing.T) {
	db := Open("D:\\leveldbtest")
	db.Put([]byte("123"), []byte("456"))

	for i := 0; i < 1000000; i++ {
		db.Put(GetRandomString(10), GetRandomString(10))
	}
	value, err := db.Get([]byte("123"))
	fmt.Println("db:", err, string(value))
	db.Close()

	db2 := Open("D:\\leveldbtest")
	value, err = db2.Get([]byte("123"))
	fmt.Println("db reopen:", err, string(value))
	db2.Close()
}
