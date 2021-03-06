package model

import (
	"fmt"
	"strings"
)

type ForeignKeyAction string

const (
	ForeignKeyActionNoAction   ForeignKeyAction = "NO_ACTION"
	ForeignKeyActionRestrict   ForeignKeyAction = "RESTRICT"
	ForeignKeyActionCascade    ForeignKeyAction = "CASCADE"
	ForeignKeyActionSetNull    ForeignKeyAction = "SET_NULL"
	ForeignKeyActionSetDefault ForeignKeyAction = "SET_DEFAULT"
)

func (self ForeignKeyAction) Equals(other ForeignKeyAction) bool {
	return strings.EqualFold(string(self), string(other))
}

type ForeignKey struct {
	Columns        DelimitedList    `xml:"columns,attr"`
	ForeignSchema  string           `xml:"foreignSchema,attr,omitempty"`
	ForeignTable   string           `xml:"foreignTable,attr"`
	ForeignColumns DelimitedList    `xml:"foreignColumns,attr,omitempty"`
	ConstraintName string           `xml:"constraintName,attr,omitempty"`
	IndexName      string           `xml:"indexName,attr,omitempty"`
	OnUpdate       ForeignKeyAction `xml:"onUpdate,attr,omitempty"`
	OnDelete       ForeignKeyAction `xml:"onDelete,attr,omitempty"`
}

func (self *ForeignKey) GetReferencedKey() KeyNames {
	cols := self.ForeignColumns
	if len(cols) == 0 {
		cols = self.Columns
	}
	return KeyNames{
		Schema:  self.ForeignSchema,
		Table:   self.ForeignTable,
		Columns: cols,
		KeyName: self.ConstraintName,
	}
}

func (self *ForeignKey) IdentityMatches(other *ForeignKey) bool {
	if self == nil || other == nil {
		return false
	}
	// TODO(go,core) validate this constraint/index name matching behavior
	// TODO(feat) case sensitivity
	return strings.EqualFold(self.ConstraintName, other.ConstraintName)
}

func (self *ForeignKey) Validate(doc *Definition, schema *Schema, table *Table) []error {
	out := []error{}
	if self.ConstraintName == "" {
		out = append(out, fmt.Errorf("foreign key in table %s.%s must have a constraint name", schema.Name, table.Name))
	}
	// TODO(go,3) validate reference, remove other codepaths
	return out
}
