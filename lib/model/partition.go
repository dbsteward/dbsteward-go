package model

import (
	"strings"
)

type TablePartitionType string

const (
	TablePartitionTypeModulo TablePartitionType = "MODULO"
)

func (self TablePartitionType) Equals(other TablePartitionType) bool {
	return strings.EqualFold(string(self), string(other))
}

type TablePartition struct {
	Type      TablePartitionType
	SqlFormat SqlFormat
	Options   []*TablePartitionOption
	Segments  []*TablePartitionSegment
}

type TablePartitionOption struct {
	Name  string
	Value string
}

type TablePartitionSegment struct {
	Name  string
	Value string
}

func (self *TablePartition) TryGetOptionValueNamed(name string) string {
	for _, option := range self.Options {
		if strings.EqualFold(option.Name, name) {
			return option.Value
		}
	}
	return ""
}
