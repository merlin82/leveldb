package memtable

import (
	"bytes"
	"encoding/binary"
)

type SequenceNumber int64
type ValueType int

const (
	kTypeDeletion ValueType = 0
	kTypeValue    ValueType = 1
)

type InternalKey struct {
	rep []byte
}

func newInternalKey(seq SequenceNumber, valueType ValueType, key, value []byte) *InternalKey {
	// Format of an entry is concatenation of:
	//  4            : key.size() + 8
	//  key bytes    : char[key.size()]
	//  8            : seq << 8 | valueType
	//  4            : value.size()
	//  value bytes  : char[value.size()]

	internalKeySize := len(key) + 8
	valueSize := len(value)
	encodedLen := 4 + internalKeySize + 4 + valueSize
	buf := make([]byte, encodedLen)

	offset := 0
	binary.LittleEndian.PutUint32(buf[offset:], uint32(internalKeySize))
	offset += 4
	copy(buf[offset:], key)
	offset += len(key)
	binary.LittleEndian.PutUint64(buf[offset:], (uint64(seq)<<8)|uint64(valueType))
	offset += 8
	binary.LittleEndian.PutUint32(buf[offset:], uint32(valueSize))
	offset += 4
	copy(buf[offset:], value)

	return &InternalKey{rep: buf}
}

func (internalKey *InternalKey) userKey() []byte {
	internalKeySize := binary.LittleEndian.Uint32(internalKey.rep)
	return internalKey.rep[4 : internalKeySize-4]
}

func (internalKey *InternalKey) userValue() []byte {
	valueOffset := binary.LittleEndian.Uint32(internalKey.rep) + 8
	return internalKey.rep[valueOffset:]
}

func (internalKey *InternalKey) valueType() ValueType {
	tagOffset := binary.LittleEndian.Uint32(internalKey.rep) - 4
	tag := binary.LittleEndian.Uint64(internalKey.rep[tagOffset:])
	return ValueType(tag & 0xff)
}

func (internalKey *InternalKey) seq() SequenceNumber {
	tagOffset := binary.LittleEndian.Uint32(internalKey.rep) - 4
	tag := binary.LittleEndian.Uint64(internalKey.rep[tagOffset:])
	return SequenceNumber(tag >> 8)
}

func LookupKey(key []byte) *InternalKey {
	buf := make([]byte, 4+len(key))
	offset := 0
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(key)))
	offset += 4
	copy(buf[offset:], key)
	return &InternalKey{rep: buf}
}

func InternalKeyComparator(a, b interface{}) int {
	// Order by:
	//    increasing user key (according to user-supplied comparator)
	//    decreasing sequence number
	//    decreasing type (though sequence# should be enough to disambiguate)
	aKey := a.(*InternalKey)
	bKey := b.(*InternalKey)
	r := UserKeyComparator(aKey.userKey(), bKey.userKey())
	if r == 0 {
		return int(aKey.seq() - bKey.seq())
	}
	return r
}

func UserKeyComparator(a, b interface{}) int {
	aKey := a.([]byte)
	bKey := b.([]byte)
	return bytes.Compare(aKey, bKey)
}
