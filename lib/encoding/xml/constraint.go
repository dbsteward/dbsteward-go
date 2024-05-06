package xml

import (
	"fmt"
	"log/slog"
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

func ConstraintsFromIR(l *slog.Logger, cs []*ir.Constraint) ([]*Constraint, error) {
	if len(cs) == 0 {
		return nil, nil
	}
	var rv []*Constraint
	for _, c := range cs {
		if c != nil {
			rv = append(
				rv,
				&Constraint{
					Name:             c.Name,
					Type:             string(c.Type),
					Definition:       c.Definition,
					ForeignIndexName: c.ForeignIndexName,
					ForeignSchema:    c.ForeignSchema,
					ForeignTable:     c.ForeignTable,
				},
			)
		}
	}
	return rv, nil
}

func (c *Constraint) ToIR() (*ir.Constraint, error) {
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

func (c *Constraint) IdentityMatches(other *Constraint) bool {
	if other == nil {
		return false
	}
	return strings.EqualFold(c.Name, other.Name)
}

func (c *Constraint) Merge(overlay *Constraint) {
	if overlay == nil {
		return
	}
	c.Type = overlay.Type
	c.Definition = overlay.Definition
}
