package util

import "fmt"

type Opt[T any] struct {
	hasValue bool
	value    T
}

func Some[T any](t T) Opt[T] {
	return Opt[T]{true, t}
}
func None[T any]() Opt[T] {
	return Opt[T]{hasValue: false}
}

func (self Opt[T]) HasValue() bool {
	return self.hasValue
}
func (self Opt[T]) Get() T {
	if self.hasValue {
		return self.value
	}
	panic("Opt.Get when no value")
}
func (self Opt[T]) GetOr(def T) T {
	if self.hasValue {
		return self.value
	}
	return def
}
func (self Opt[T]) GetOrZero() T {
	return self.value // value will be zero if not hasValue
}

func (self Opt[T]) Maybe() (T, bool) {
	return self.value, self.hasValue
}

func (self Opt[T]) Ptr() *T {
	if self.hasValue {
		return &self.value
	}
	return nil
}

func (self Opt[T]) Equals(other Opt[T]) bool {
	if self.HasValue() != other.HasValue() {
		// None != Some
		return false
	}
	if !self.HasValue() {
		// we now know self.HasValue() == other.HasValue(), so if !self.HasValue() then we're done
		return true
	}
	if t, ok := (any)(self.value).(interface{ Equals(T) bool }); ok {
		// we need to do a runtime check to see if T implements Equals(T) because we can't specialize T in go 1.18
		return t.Equals(other.value)
	}
	panic(fmt.Sprintf("Type %T does not implement Equals(%T)", self.value, self.value))
}
