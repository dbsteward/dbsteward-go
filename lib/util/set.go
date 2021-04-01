package util

// TODO(go,nth) see if there's places in the code we can trivially upgrade to use a Set
// instead of the assortment of util set union/diff/intersect functions
// TODO(go,nth) make this threadsafe

type Set struct {
	m  map[interface{}]interface{}
	id IdFunc
}

func NewSet(id IdFunc) *Set {
	return &Set{
		m:  map[interface{}]interface{}{},
		id: id,
	}
}

func (self *Set) Add(items ...interface{}) {
	self.AddFrom(items)
}

func (self *Set) AddFrom(items []interface{}) {
	for _, item := range items {
		self.m[self.id(item)] = item
	}
}

func (self *Set) Remove(items ...interface{}) {
	self.RemoveFrom(items)
}

func (self *Set) RemoveFrom(items []interface{}) {
	for _, item := range items {
		delete(self.m, self.id(item))
	}
}

func (self *Set) Has(item interface{}) bool {
	_, ok := self.m[self.id(item)]
	return ok
}

func (self *Set) Items() []interface{} {
	out := make([]interface{}, 0, len(self.m))
	for _, item := range self.m {
		out = append(out, item)
	}
	return out
}
