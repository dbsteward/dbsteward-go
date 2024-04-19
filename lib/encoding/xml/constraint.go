package xml

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/ir"
)

type Constraint struct {
	Name             string `xml:"name,attr,omitempty"`
	Type             string `xml:"type,attr,omitempty"`
	Definition       string `xml:"definition,attr,omitempty"`
	ForeignIndexName string `xml:"foreignIndexName,attr,omitempty"`
	ForeignSchema    string `xml:"foreignSchema,attr,omitempty"`
	ForeignTable     string `xml:"foreignTable,attr,omitempty"`
}

func (c *Constraint) ToModel() (*ir.Constraint, error) {
	rv := ir.Constraint{
		Name:             c.Name,
		Definition:       c.Definition,
		ForeignIndexName: c.ForeignIndexName,
		ForeignSchema:    c.ForeignSchema,
		ForeignTable:     c.ForeignTable,
	}
	var err error
	rv.Type, err = ir.NewConstraintType(c.Type)
	if err != nil {
		return nil, fmt.Errorf("invalid constraint '%s': %w", c.Name, err)
	}
	return &rv, nil
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
