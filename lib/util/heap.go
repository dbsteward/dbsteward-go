package util

import (
	"container/heap"
	"strings"
)

// TODO(go,core) upgrade to generics when go 1.18 comes out

type IdFunc = func(interface{}) interface{}
type LessFunc = func(l, r interface{}) bool

func IdentityId(x interface{}) interface{} {
	return x
}
func StrLowerId(x interface{}) interface{} {
	return strings.ToLower(x.(string))
}

// CountHeap is a type of heap that counts the number of times a given item has been added
// The top of the heap will be the item that has been seen the most
type CountHeap struct {
	h  *Heap
	id IdFunc
	m  map[interface{}]*countHeapItem
}

type countHeapItem struct {
	value interface{}
	count int
	index int
}

func NewCountHeap(id IdFunc) *CountHeap {
	less := func(l, r interface{}) bool {
		// reverse the equality so the top of the heap is the max, not the min
		return l.(*countHeapItem).count > r.(*countHeapItem).count
	}
	update := func(item interface{}, index int) {
		item.(*countHeapItem).index = index
	}
	return &CountHeap{
		h:  NewHeapWithUpdate(less, update),
		id: id,
		m:  map[interface{}]*countHeapItem{},
	}
}

func (self *CountHeap) Len() int {
	return self.h.Len()
}

func (self *CountHeap) Push(x interface{}) {
	id := self.id(x)
	if item, ok := self.m[id]; ok {
		item.count += 1
		self.h.Updated(item.index)
	} else {
		item := &countHeapItem{
			value: x,
			count: 1,
		}
		self.m[id] = item
		self.h.Push(item)
	}
}

func (self *CountHeap) Pop() interface{} {
	return self.pop().value
}

func (self *CountHeap) PopCount() (interface{}, int) {
	item := self.pop()
	return item.value, item.count
}

func (self *CountHeap) PopAll() []interface{} {
	out := make([]interface{}, self.Len())
	for i := 0; self.Len() > 0; i += 1 {
		out[i] = self.Pop()
	}
	return out
}

func (self *CountHeap) pop() *countHeapItem {
	item := self.h.Pop().(*countHeapItem)
	id := self.id(item.value)
	delete(self.m, id)
	return item
}

// Implements a general purpose (min) Heap data structure, based on user-defined ordering.
// Items are `interface{}`, so callers must cast to the desired type when retrieving
// Items are guaranteed to be whatever the caller puts in: garbage in, garbage out.
type Heap struct {
	h *heapImpl
}

func NewHeap(less LessFunc) *Heap {
	h := &Heap{
		&heapImpl{less: less},
	}
	heap.Init(h.h)
	return h
}

// Advanced: as NewHeap, but `update` will be called with the item and its new index when it changes
func NewHeapWithUpdate(less LessFunc, update func(interface{}, int)) *Heap {
	h := NewHeap(less)
	h.h.update = update
	return h
}

func (self *Heap) Len() int {
	return self.h.Len()
}

// Pushes an item into the heap; smallest items will ripple to the bottom
func (self *Heap) Push(x interface{}) {
	heap.Push(self.h, x)
}

// Removes and returns the smallest item from the heap
func (self *Heap) Pop() interface{} {
	return heap.Pop(self.h)
}

// Returns the smallest item on the heap, but does not remove it
func (self *Heap) Peek() interface{} {
	return self.h.Peek()
}

// Advanced: Notifies the heap that the ordering of the item at index i has changed
func (self *Heap) Updated(i int) {
	heap.Fix(self.h, i)
}

// heapImpl contains the actual container/heap.Interface-conforming
// interface to the heap. Heap above is the high-level user-facing
// interface

type heapImpl struct {
	items  []interface{}
	less   LessFunc
	update func(interface{}, int)
}

func (self *heapImpl) Len() int {
	return len(self.items)
}
func (self *heapImpl) Less(i, j int) bool {
	return self.less(self.items[i], self.items[j])
}
func (self *heapImpl) Swap(i, j int) {
	self.items[i], self.items[j] = self.items[j], self.items[i]
	if self.update != nil {
		self.update(self.items[i], i)
		self.update(self.items[j], j)
	}
}
func (self *heapImpl) Push(x interface{}) {
	self.items = append(self.items, x)
	if self.update != nil {
		self.update(x, len(self.items)-1)
	}
}
func (self *heapImpl) Pop() interface{} {
	// get the last one
	last := len(self.items) - 1
	item := self.items[last]
	// then set it nil to garbage collect the reference
	self.items[last] = nil
	// then reassign a 1-shorter slice over top to "shrink" the slice
	self.items = self.items[0:last]
	if self.update != nil {
		self.update(item, -1)
	}
	return item
}
func (self *heapImpl) Peek() interface{} {
	return self.items[0]
}
