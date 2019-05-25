package internal

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
)

type ValueType int8

const (
	TypeDeletion ValueType = 0
	TypeValue    ValueType = 1
)

type InternalKey struct {
	Seq       uint64
	Type      ValueType
	UserKey   []byte
	UserValue []byte
}

func NewInternalKey(seq uint64, valueType ValueType, key, value []byte) *InternalKey {
	var internalKey InternalKey
	internalKey.Seq = seq
	internalKey.Type = valueType

	internalKey.UserKey = make([]byte, len(key))
	copy(internalKey.UserKey, key)
	internalKey.UserValue = make([]byte, len(value))
	copy(internalKey.UserValue, value)

	return &internalKey
}

func (key *InternalKey) EncodeTo(w io.Writer) error {
	binary.Write(w, binary.LittleEndian, key.Seq)
	binary.Write(w, binary.LittleEndian, key.Type)
	binary.Write(w, binary.LittleEndian, int32(len(key.UserKey)))
	binary.Write(w, binary.LittleEndian, key.UserKey)
	binary.Write(w, binary.LittleEndian, int32(len(key.UserValue)))
	return binary.Write(w, binary.LittleEndian, key.UserValue)
}

func (key *InternalKey) DecodeFrom(r io.Reader) error {
	var tmp int32
	binary.Read(r, binary.LittleEndian, &key.Seq)
	binary.Read(r, binary.LittleEndian, &key.Type)
	binary.Read(r, binary.LittleEndian, &tmp)
	key.UserKey = make([]byte, tmp)
	binary.Read(r, binary.LittleEndian, key.UserKey)
	binary.Read(r, binary.LittleEndian, &tmp)
	key.UserValue = make([]byte, tmp)
	return binary.Read(r, binary.LittleEndian, key.UserValue)
}

func LookupKey(key []byte) *InternalKey {
	return NewInternalKey(math.MaxUint64, TypeValue, key, nil)
}

func InternalKeyComparator(a, b interface{}) int {
	// Order by:
	//    increasing user key (according to user-supplied comparator)
	//    decreasing sequence number
	//    decreasing type (though sequence# should be enough to disambiguate)
	aKey := a.(*InternalKey)
	bKey := b.(*InternalKey)
	r := UserKeyComparator(aKey.UserKey, bKey.UserKey)
	if r == 0 {
		anum := aKey.Seq
		bnum := bKey.Seq
		if anum > bnum {
			r = -1
		} else if anum < bnum {
			r = +1
		}
	}
	return r
}

func UserKeyComparator(a, b interface{}) int {
	aKey := a.([]byte)
	bKey := b.([]byte)
	return bytes.Compare(aKey, bKey)
}
