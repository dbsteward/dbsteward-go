package util

type EqualFunc[T comparable] func(l, r T) bool

func StrictEqual[T comparable](l, r T) bool {
	return l == r
}

func Partial2[A, B, R any](f func(A, B) R, a A) func(B) R {
	return func(b B) R {
		return f(a, b)
	}
}
func Partial2R[A, B, R any](f func(A, B) R, b B) func(A) R {
	return func(a A) R {
		return f(a, b)
	}
}
