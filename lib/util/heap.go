package util

import (
	"container/heap"
)

type IdFunc[T any, ID comparable] func(T) ID
type LessFunc[T any] func(l, r T) bool

func IdentityId[T comparable](x T) T {
	return x
}

// CountHeap is a type of heap that counts the number of times a given item has been added
// The top of the heap will be the item that has been seen the most
type CountHeap[T any, ID comparable] struct {
	h  *Heap[*countHeapItem[T]]
	id IdFunc[T, ID]
	m  map[ID]*countHeapItem[T]
}

type countHeapItem[T any] struct {
	value T
	count int
	index int
}

func NewCountHeap[T any, ID comparable](id IdFunc[T, ID]) *CountHeap[T, ID] {
	less := func(l, r *countHeapItem[T]) bool {
		// reverse the equality so the top of the heap is the max, not the min
		return l.count > r.count
	}
	update := func(item *countHeapItem[T], index int) {
		item.index = index
	}
	return &CountHeap[T, ID]{
		h:  NewHeapWithUpdate(less, update),
		id: id,
		m:  map[ID]*countHeapItem[T]{},
	}
}

func (self *CountHeap[T, ID]) Len() int {
	return self.h.Len()
}

func (self *CountHeap[T, ID]) Push(x T) {
	id := self.id(x)
	if item, ok := self.m[id]; ok {
		item.count += 1
		self.h.Updated(item.index)
	} else {
		item := &countHeapItem[T]{
			value: x,
			count: 1,
		}
		self.m[id] = item
		self.h.Push(item)
	}
}

func (self *CountHeap[T, ID]) Pop() T {
	return self.pop().value
}

func (self *CountHeap[T, ID]) PopCount() (T, int) {
	item := self.pop()
	return item.value, item.count
}

func (self *CountHeap[T, ID]) PopAll() []T {
	out := make([]T, self.Len())
	for i := 0; self.Len() > 0; i += 1 {
		out[i] = self.Pop()
	}
	return out
}

func (self *CountHeap[T, ID]) pop() *countHeapItem[T] {
	item := self.h.Pop().(*countHeapItem[T])
	id := self.id(item.value)
	delete(self.m, id)
	return item
}

// Implements a general purpose (min) Heap data structure, based on user-defined ordering.
// Items are `interface{}`, so callers must cast to the desired type when retrieving
// Items are guaranteed to be whatever the caller puts in: garbage in, garbage out.
type Heap[T any] struct {
	h *heapImpl[T]
}

func NewHeap[T any](less LessFunc[T]) *Heap[T] {
	h := &Heap[T]{
		&heapImpl[T]{less: less},
	}
	heap.Init(h.h)
	return h
}

// Advanced: as NewHeap, but `update` will be called with the item and its new index when it changes
func NewHeapWithUpdate[T any](less LessFunc[T], update func(T, int)) *Heap[T] {
	h := NewHeap(less)
	h.h.update = update
	return h
}

func (self *Heap[T]) Len() int {
	return self.h.Len()
}

// Pushes an item into the heap; smallest items will ripple to the bottom
func (self *Heap[T]) Push(x interface{}) {
	heap.Push(self.h, x)
}

// Removes and returns the smallest item from the heap
func (self *Heap[T]) Pop() interface{} {
	return heap.Pop(self.h)
}

// Returns the smallest item on the heap, but does not remove it
func (self *Heap[T]) Peek() interface{} {
	return self.h.Peek()
}

// Advanced: Notifies the heap that the ordering of the item at index i has changed
func (self *Heap[T]) Updated(i int) {
	heap.Fix(self.h, i)
}

// heapImpl contains the actual container/heap.Interface-conforming
// interface to the heap. Heap above is the high-level user-facing
// interface

type heapImpl[T any] struct {
	items  []interface{}
	less   LessFunc[T]
	update func(T, int)
}

func (self *heapImpl[T]) Len() int {
	return len(self.items)
}
func (self *heapImpl[T]) Less(i, j int) bool {
	return self.less(self.items[i].(T), self.items[j].(T))
}
func (self *heapImpl[T]) Swap(i, j int) {
	self.items[i], self.items[j] = self.items[j], self.items[i]
	if self.update != nil {
		self.update(self.items[i].(T), i)
		self.update(self.items[j].(T), j)
	}
}
func (self *heapImpl[T]) Push(x interface{}) {
	self.items = append(self.items, x)
	if self.update != nil {
		self.update(x.(T), len(self.items)-1)
	}
}
func (self *heapImpl[T]) Pop() interface{} {
	// get the last one
	last := len(self.items) - 1
	item := self.items[last]
	// then set it nil to garbage collect the reference
	self.items[last] = nil
	// then reassign a 1-shorter slice over top to "shrink" the slice
	self.items = self.items[0:last]
	if self.update != nil {
		self.update(item.(T), -1)
	}
	return item
}
func (self *heapImpl[T]) Peek() interface{} {
	return self.items[0]
}
