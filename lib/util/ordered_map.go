package util

import (
	"strings"
	"fmt"
)

type EqualFunc = func(l, r interface{}) bool

func StrictEqual(l, r interface{}) bool {
	return l == r
}
func IStrEqual(l, r interface{}) bool {
	return strings.EqualFold(l.(string), r.(string))
}

// TODO(go,core) find map iterations that should be deterministic and upgrade to use this impl
// TODO(go,nth) make this threadsafe

// OrderedMap implements a simple map data structure that maintains its insertion order.
type OrderedMap struct {
	data map[interface{}]interface{}
	ind map[int]interface{}
}

func NewOrderedMap() *OrderedMap {
	return &OrderedMap{
		data: make(map[interface{}]interface{}),
		ind: make(map[int]interface{}),
	}
}
func NewOrderedMapOfSize(n int) *OrderedMap {
	return &OrderedMap{
		data: make(map[interface{}]interface{}, n),
		ind: make(map[int]interface{}, n),
	}
}

func (self *OrderedMap) ShallowClone() *OrderedMap {
	out := NewOrderedMapOfSize(self.Len())
	for i, k := range self.ind {
		out.ind[i] = k
		out.data[k] = self.data[k]
	}
	return out
}
func (self *OrderedMap) Len() int {
	return len(self.data)
}
func (self *OrderedMap) Insert(keyvals ...interface{}) *OrderedMap {
	if len(keyvals) == 0 || len(keyvals) % 2 != 0 {
		panic(fmt.Errorf("Expected non-zero even number of key/value pairs to OrderedMap.Insert, got: %v", keyvals))
	}
	for i, ii := 0, len(keyvals); i < ii; i+=2 {
		idx := len(self.data)
		self.ind[idx] = keyvals[i]
		self.data[keyvals[i]] = keyvals[i+1]
	}
	return self
}
func (self *OrderedMap) Get(key interface{}) interface{} {
	return self.data[key]
}
func (self *OrderedMap) GetIndex(idx int) (interface{}, interface{}) {
	l := self.Len();
	if idx < 0 || idx >= l {
		panic(fmt.Errorf("Bounds check: OrderedMap.GetIndex(%d) on map of len %d", idx, l))
	}
	k := self.ind[idx]
	return k, self.data[k]
}
func (self *OrderedMap) Delete(key interface{}) interface{} {
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
func (self *OrderedMap) ForEach(f func(i int, key, val interface{})) {
	for i, ii := 0, self.Len(); i < ii; i++ {
		key, val := self.GetIndex(i)
		f(i, key, val)
	}
}

func (self *OrderedMap) Keys() []interface{} {
	out := make([]interface{}, self.Len())
	self.ForEach(func(i int, key, val interface{}) {
		out[i] = key
	})
	return out
}

func (self *OrderedMap) Values() []interface{} {
	out := make([]interface{}, self.Len())
	self.ForEach(func(i int, key, val interface{}) {
		out[i] = val
	})
	return out
}

func (self *OrderedMap) Entries() [][]interface{} {
	out := make([][]interface{}, self.Len())
	self.ForEach(func(i int, key, val interface{}) {
		out[i] = []interface{}{key, val}
	})
	return out
}

func (self *OrderedMap) MapPleaseDoNotMutate() map[interface{}]interface{} {
	return self.data
}

func (left *OrderedMap) Difference(right *OrderedMap) *OrderedMap {
	return left.DifferenceFunc(right, StrictEqual)
}
func (left *OrderedMap) DifferenceIStr(right *OrderedMap) *OrderedMap {
	return left.DifferenceFunc(right, IStrEqual)
}
func (left *OrderedMap) DifferenceFunc(right *OrderedMap, keyEq EqualFunc) *OrderedMap {
	out := NewOrderedMap()
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

func (left *OrderedMap) Intersect(right *OrderedMap) *OrderedMap {
	return left.IntersectFunc(right, StrictEqual)
}
func (left *OrderedMap) IntersectIStr(right *OrderedMap) *OrderedMap {
	return left.IntersectFunc(right, IStrEqual)
}
func (left *OrderedMap) IntersectFunc(right *OrderedMap, keyEq EqualFunc) *OrderedMap {
	out := NewOrderedMap()
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

func (left *OrderedMap) Union(right *OrderedMap) *OrderedMap {
	return left.UnionFunc(right, StrictEqual)
}
func (left *OrderedMap) UnionIStr(right *OrderedMap) *OrderedMap {
	return left.UnionFunc(right, IStrEqual)
}
func (left *OrderedMap) UnionFunc(right *OrderedMap, keyEq EqualFunc) *OrderedMap {
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
