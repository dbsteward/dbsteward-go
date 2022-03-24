package util

func Ptr[T any](t T) *T {
	return &t
}

// nil == nil, &x == &x, everything else !=
func PtrEq[T comparable](a, b *T) bool {
	if a == nil && b == nil {
		return true
	}
	if a != nil && b != nil {
		return *a == *b
	}
	return false
}
