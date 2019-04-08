package format

import (
	"bytes"
	"math"
)

type ValueType int

const (
	TypeDeletion ValueType = 0
	TypeValue    ValueType = 1
)

type InternalKey struct {
	Seq       int64
	Type      ValueType
	UserKey   []byte
	UserValue []byte
}

func NewInternalKey(seq int64, valueType ValueType, key, value []byte) *InternalKey {
	var internalKey InternalKey
	internalKey.Seq = seq
	internalKey.Type = valueType
	internalKey.UserKey = make([]byte, len(key))
	copy(internalKey.UserKey, key)
	internalKey.UserValue = make([]byte, len(value))
	copy(internalKey.UserValue, value)

	return &internalKey
}

func LookupKey(key []byte) *InternalKey {
	return NewInternalKey(math.MaxInt64, TypeValue, key, nil)
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
