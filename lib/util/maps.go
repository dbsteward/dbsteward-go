package util

// returns a map containing keys from left that are not in right, using a custom key-equality function
func DifferenceMapKeysFunc[M ~map[K]V, K comparable, V any](left, right M, eq EqualFunc[K]) M {
	out := M{}
	for l, lv := range left {
		inRight := false
		for r := range right {
			if eq(l, r) {
				inRight = true
				break
			}
		}
		if !inRight {
			out[l] = lv
		}
	}
	return out
}

// returns keys from left that are also in right, using a custom key-equality function
func IntersectMapKeysFunc[M ~map[K]V, K comparable, V any](left, right M, eq EqualFunc[K]) M {
	out := M{}
	for l, lv := range left {
		for r := range right {
			if eq(l, r) {
				out[l] = lv
				break
			}
		}
	}
	return out
}

func UnionMapKeysFunc[M ~map[K]V, K comparable, V any](left, right M, eq EqualFunc[K]) M {
	out := M{}
	for l, lv := range left {
		out[l] = lv
	}
	for r, rv := range right {
		found := false
		for o := range out {
			if eq(r, o) {
				found = true
				break
			}
		}
		if !found {
			out[r] = rv
		}
	}
	return out
}

func MapKeys[M ~map[K]V, K comparable, V any](m M) []K {
	out := make([]K, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
