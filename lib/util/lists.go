package util

import "strings"

func Contains[S ~[]T, T comparable](list S, target T) bool {
	return IndexOf(list, target) >= 0
}

func ContainsFunc[S ~[]T, T comparable](list S, target T, eq EqualFunc[T]) bool {
	return IndexOfFunc(list, target, eq) >= 0
}

func IndexOf[S ~[]T, T comparable](list S, target T) int {
	return IndexOfFunc(list, target, StrictEqual[T])
}

func IndexOfFunc[S ~[]T, T comparable](list S, target T, eq EqualFunc[T]) int {
	for i, el := range list {
		if eq(el, target) {
			return i
		}
	}
	return -1
}

func Find[S ~[]T, T any](list S, pred func(T) bool) Opt[T] {
	for _, t := range list {
		if pred(t) {
			return Some(t)
		}
	}
	return None[T]()
}

func FindBy[S ~[]T, T any, K comparable](list S, id K, get func(T) K) Opt[T] {
	return Find(list, func(t T) bool {
		return id == get(t)
	})
}

func FindNamed[S ~[]T, T interface{ Name() string }](list S, name string) Opt[T] {
	return Find(list, func(t T) bool {
		return strings.EqualFold(t.Name(), name)
	})
}

func FindMatching[S ~[]T, T interface{ IdentityMatches(T) bool }](list S, other T) Opt[T] {
	return Find(list, func(t T) bool {
		return t.IdentityMatches(other)
	})
}

// UnionFunc unions two lists, using the provided equality function
// TODO(go,nth) optimize this using a map implementation
func UnionFunc[S ~[]T, T comparable](left, right S, eq EqualFunc[T]) S {
	// no, this is not the most efficient, but it is the simplest
	out := make(S, len(left))
	copy(out, left)
	for _, r := range right {
		if !ContainsFunc(out, r, eq) {
			out = append(out, r)
		}
	}
	return out
}

// IntersectFunc intersects two lists, using the provided equality function
// only returns the strings that are present in both lists
// will use the values from the left side, in the case that the values differ in case
// if a string is present multiple times in a list, it will be duplicated in the output
// TODO(go,nth) optimize this using a map implementation
func IntersectFunc[S ~[]T, T comparable](left, right S, eq EqualFunc[T]) S {
	// no, this is not the most efficient, but it is the simplest
	out := S{}
	for _, l := range left {
		for _, r := range right {
			if eq(l, r) {
				out = append(out, l)
			}
		}
	}
	return out
}

// DifferenceFunc removes any elements from `right` from the `left` list
// TODO(go,nth) optimize this using a map implementation
func DifferenceFunc[S ~[]T, T comparable](left, right S, eq EqualFunc[T]) S {
	out := S{}
outer:
	for _, l := range left {
		for _, r := range right {
			if eq(l, r) {
				continue outer
			}
		}
		out = append(out, l)
	}
	return out
}

func Remove[S ~[]T, T comparable](slice S, target T) S {
	return RemoveFunc(slice, target, StrictEqual[T])
}

func RemoveFunc[S ~[]T, T comparable](slice S, target T, eq EqualFunc[T]) S {
	// a quick helper to cut down on complexity below, see https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	// HACK: this is, IMHO, a really bullshit and footgunny method to do this efficiently
	b := slice[:0]
	for _, x := range slice {
		if !eq(x, target) {
			b = append(b, x)
		}
	}
	// garbage collect
	for i := len(b); i < len(slice); i++ {
		slice[i] = ZeroVal[T]()
	}
	return b
}

func Map[S ~[]T, T, U any](slice S, f func(T) U) []U {
	out := make([]U, len(slice))
	for i, t := range slice {
		out[i] = f(t)
	}
	return out
}

func MapErr[S ~[]T, T, U any](slice S, f func(T) (U, error)) ([]U, error) {
	out := make([]U, len(slice))
	for i, t := range slice {
		u, err := f(t)
		out[i] = u
		if err != nil {
			return out, err
		}
	}
	return out, nil
}
