package xml

import (
	"log/slog"

	"github.com/dbsteward/dbsteward/lib/ir"
)

type Grant struct {
	Roles       DelimitedList      `xml:"role,attr,omitempty"`
	Permissions CommaDelimitedList `xml:"operation,attr,omitempty"`
	With        string             `xml:"with,attr,omitempty"`
}

func GrantsFromIR(l *slog.Logger, gs []*ir.Grant) ([]*Grant, error) {
	if len(gs) == 0 {
		return nil, nil
	}
	var rv []*Grant
	for _, g := range gs {
		if g != nil {
			rv = append(
				rv,
				&Grant{
					Roles:       g.Roles,
					Permissions: g.Permissions,
					With:        g.With,
				},
			)
		}
	}
	return rv, nil
}

func (g *Grant) ToIR() (*ir.Grant, error) {
	rv := ir.Grant{
		Roles:       g.Roles,
		Permissions: g.Permissions,
		With:        g.With,
	}
	return &rv, nil
}
