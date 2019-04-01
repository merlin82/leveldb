package utils

//    -1 , if a < b
//    0  , if a == b
//    1  , if a > b
type Comparator func(a, b interface{}) int

func IntComparator(a, b interface{}) int {
	aAsserted := a.(int)
	bAsserted := b.(int)
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}
