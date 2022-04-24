package xml

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
	Name             string         `xml:"name,attr,omitempty"`
	Type             ConstraintType `xml:"type,attr,omitempty"`
	Definition       string         `xml:"definition,attr,omitempty"`
	ForeignIndexName string         `xml:"foreignIndexName,attr,omitempty"`
	ForeignSchema    string         `xml:"foreignSchema,attr,omitempty"`
	ForeignTable     string         `xml:"foreignTable,attr,omitempty"`
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

func (self *Constraint) Validate(*Definition, *Schema, *Table) []error {
	// TODO(go,3) validate values
	return nil
}
