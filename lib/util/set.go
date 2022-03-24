package util

// TODO(go,nth) see if there's places in the code we can trivially upgrade to use a Set
// instead of the assortment of util set union/diff/intersect functions
// TODO(go,nth) make this threadsafe

type Set[T any, ID comparable] struct {
	m  map[ID]T
	id IdFunc[T, ID]
}

func NewSet[T any, ID comparable](id IdFunc[T, ID]) *Set[T, ID] {
	return &Set[T, ID]{
		m:  map[ID]T{},
		id: id,
	}
}

func (self *Set[T, ID]) Add(items ...T) {
	self.AddFrom(items)
}

func (self *Set[T, ID]) AddFrom(items []T) {
	for _, item := range items {
		self.m[self.id(item)] = item
	}
}

func (self *Set[T, ID]) Remove(items ...T) {
	self.RemoveFrom(items)
}

func (self *Set[T, ID]) RemoveFrom(items []T) {
	for _, item := range items {
		delete(self.m, self.id(item))
	}
}

func (self *Set[T, ID]) Has(item T) bool {
	_, ok := self.m[self.id(item)]
	return ok
}

func (self *Set[T, ID]) Items() []T {
	out := make([]T, 0, len(self.m))
	for _, item := range self.m {
		out = append(out, item)
	}
	return out
}
