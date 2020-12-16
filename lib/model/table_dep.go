package model

// TODO(go,nth) is there a better place to put this?

type TableDepEntry struct {
	Schema      *Schema
	Table       *Table
	IgnoreEntry bool
}
