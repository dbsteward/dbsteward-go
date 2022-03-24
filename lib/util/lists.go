package util

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
