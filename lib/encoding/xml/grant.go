package xml

import "github.com/dbsteward/dbsteward/lib/ir"

type Grant struct {
	Roles       DelimitedList      `xml:"role,attr,omitempty"`
	Permissions CommaDelimitedList `xml:"operation,attr,omitempty"`
	With        string             `xml:"with,attr,omitempty"`
}

func (g *Grant) ToModel() (*ir.Grant, error) {
	rv := ir.Grant{
		Roles:       g.Roles,
		Permissions: g.Permissions,
		With:        g.With,
	}
	return &rv, nil
}
