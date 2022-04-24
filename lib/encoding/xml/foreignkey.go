package xml

import (
	"strings"
)

type ForeignKey struct {
	Columns        DelimitedList `xml:"columns,attr"`
	ForeignSchema  string        `xml:"foreignSchema,attr,omitempty"`
	ForeignTable   string        `xml:"foreignTable,attr"`
	ForeignColumns DelimitedList `xml:"foreignColumns,attr,omitempty"`
	ConstraintName string        `xml:"constraintName,attr,omitempty"`
	IndexName      string        `xml:"indexName,attr,omitempty"`
	OnUpdate       string        `xml:"onUpdate,attr,omitempty"`
	OnDelete       string        `xml:"onDelete,attr,omitempty"`
}

func (self *ForeignKey) IdentityMatches(other *ForeignKey) bool {
	if self == nil || other == nil {
		return false
	}
	// TODO(go,core) validate this constraint/index name matching behavior
	// TODO(feat) case sensitivity
	return strings.EqualFold(self.ConstraintName, other.ConstraintName)
}
