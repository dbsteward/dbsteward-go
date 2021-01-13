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
	Type      TablePartitionType       `xml:"type,attr"`
	SqlFormat SqlFormat                `xml:"sqlFormat,attr"`
	Options   []*TablePartitionOption  `xml:"tablePartitionOption"`
	Segments  []*TablePartitionSegment `xml:"tablePartitionSegment"`
}

type TablePartitionOption struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

type TablePartitionSegment struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

func (self *TablePartition) TryGetOptionValueNamed(name string) string {
	for _, option := range self.Options {
		if strings.EqualFold(option.Name, name) {
			return option.Value
		}
	}
	return ""
}
