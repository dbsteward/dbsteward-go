package model

import "strings"

type ConstraintType string

const (
	ConstraintTypeCheck   ConstraintType = "CHECK"
	ConstraintTypeUnique  ConstraintType = "UNIQUE"
	ConstraintTypeForeign ConstraintType = "FOREIGN KEY"
)

func (self ConstraintType) Equals(other ConstraintType) bool {
	return strings.EqualFold(string(self), string(other))
}

type Constraint struct {
	Name             string         `xml:"name,attr"`
	Type             ConstraintType `xml:"type,attr"`
	Definition       string         `xml:"definition,attr"`
	ForeignIndexName string         `xml:"foreignIndexName,attr"`
	ForeignSchema    string         `xml:"foreignSchema,attr"`
	ForeignTable     string         `xml:"foreignTable,attr"`
}

func (self *Constraint) IdentityMatches(other *Constraint) bool {
	if other == nil {
		return false
	}
	return strings.EqualFold(self.Name, other.Name)
}

func (self *Constraint) Merge(overlay *Constraint) {
	if overlay == nil {
		return
	}
	self.Type = overlay.Type
	self.Definition = overlay.Definition
}
