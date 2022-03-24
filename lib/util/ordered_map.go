package util

import (
	"fmt"
)

type EqualFunc[T comparable] func(l, r T) bool

func StrictEqual[T comparable](l, r T) bool {
	return l == r
}

// TODO(go,core) find map iterations that should be deterministic and upgrade to use this impl
// TODO(go,nth) make this threadsafe

// OrderedMap implements a simple map data structure that maintains its insertion order.
type OrderedMap[K comparable, V any] struct {
	data map[K]V
	ind  map[int]K
}

func NewOrderedMap[K comparable, V any]() *OrderedMap[K, V] {
	return &OrderedMap[K, V]{
		data: make(map[K]V),
		ind:  make(map[int]K),
	}
}
func NewOrderedMapOfSize[K comparable, V any](n int) *OrderedMap[K, V] {
	return &OrderedMap[K, V]{
		data: make(map[K]V, n),
		ind:  make(map[int]K, n),
	}
}

func (self *OrderedMap[K, V]) ShallowClone() *OrderedMap[K, V] {
	out := NewOrderedMapOfSize[K, V](self.Len())
	for i, k := range self.ind {
		out.ind[i] = k
		out.data[k] = self.data[k]
	}
	return out
}
func (self *OrderedMap[K, V]) Len() int {
	return len(self.data)
}
func (self *OrderedMap[K, V]) Insert(keyvals ...interface{}) *OrderedMap[K, V] {
	if len(keyvals) == 0 || len(keyvals)%2 != 0 {
		panic(fmt.Errorf("Expected non-zero even number of key/value pairs to OrderedMap.Insert, got: %v", keyvals))
	}
	for i, ii := 0, len(keyvals); i < ii; i += 2 {
		k := keyvals[i].(K)
		v := keyvals[i+1].(V)
		idx := len(self.data)
		self.ind[idx] = k
		self.data[k] = v
	}
	return self
}
func (self *OrderedMap[K, V]) Get(key K) V {
	return self.data[key]
}
func (self *OrderedMap[K, V]) GetIndex(idx int) (K, V) {
	l := self.Len()
	if idx < 0 || idx >= l {
		panic(fmt.Errorf("Bounds check: OrderedMap.GetIndex(%d) on map of len %d", idx, l))
	}
	k := self.ind[idx]
	return k, self.data[k]
}
func (self *OrderedMap[K, V]) Delete(key K) V {
	v := self.data[key]
	delete(self.data, key)
	for i, k := range self.ind {
		if k == key {
			delete(self.ind, i)
			return v
		}
	}
	panic("self.data and self.ind are out of sync")
}
func (self *OrderedMap[K, V]) ForEach(f func(i int, key K, val V)) {
	for i, ii := 0, self.Len(); i < ii; i++ {
		key, val := self.GetIndex(i)
		f(i, key, val)
	}
}

func (self *OrderedMap[K, V]) Keys() []K {
	out := make([]K, self.Len())
	self.ForEach(func(i int, key K, val V) {
		out[i] = key
	})
	return out
}

func (self *OrderedMap[K, V]) Values() []V {
	out := make([]V, self.Len())
	self.ForEach(func(i int, key K, val V) {
		out[i] = val
	})
	return out
}

type Entry[K comparable, V any] struct {
	Key   K
	Value V
}

func (self *OrderedMap[K, V]) Entries() []Entry[K, V] {
	out := make([]Entry[K, V], self.Len())
	self.ForEach(func(i int, key K, val V) {
		out[i] = Entry[K, V]{Key: key, Value: val}
	})
	return out
}

func (self *OrderedMap[K, V]) MapPleaseDoNotMutate() map[K]V {
	return self.data
}

func (left *OrderedMap[K, V]) Difference(right *OrderedMap[K, V]) *OrderedMap[K, V] {
	return left.DifferenceFunc(right, StrictEqual[K])
}
func (left *OrderedMap[K, V]) DifferenceFunc(right *OrderedMap[K, V], keyEq EqualFunc[K]) *OrderedMap[K, V] {
	out := NewOrderedMap[K, V]()
	for _, lk := range left.Keys() {
		inRight := false
		for _, rk := range right.Keys() {
			if keyEq(lk, rk) {
				inRight = true
				break
			}
		}
		if !inRight {
			out.Insert(lk, left.Get(lk))
		}
	}
	return out
}

func (left *OrderedMap[K, V]) Intersect(right *OrderedMap[K, V]) *OrderedMap[K, V] {
	return left.IntersectFunc(right, StrictEqual[K])
}
func (left *OrderedMap[K, V]) IntersectFunc(right *OrderedMap[K, V], keyEq EqualFunc[K]) *OrderedMap[K, V] {
	out := NewOrderedMap[K, V]()
	for _, lk := range left.Keys() {
		for _, rk := range right.Keys() {
			if keyEq(lk, rk) {
				out.Insert(lk, left.Get(lk))
				break
			}
		}
	}
	return out
}

func (left *OrderedMap[K, V]) Union(right *OrderedMap[K, V]) *OrderedMap[K, V] {
	return left.UnionFunc(right, StrictEqual[K])
}
func (left *OrderedMap[K, V]) UnionFunc(right *OrderedMap[K, V], keyEq EqualFunc[K]) *OrderedMap[K, V] {
	out := left.ShallowClone()
	for _, rk := range right.Keys() {
		found := false
		for _, ok := range out.Keys() {
			if keyEq(rk, ok) {
				found = true
				break
			}
		}
		if !found {
			out.Insert(rk, right.Get(rk))
		}
	}
	return out
}
