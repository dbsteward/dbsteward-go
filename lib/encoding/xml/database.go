package xml

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/ir"
)

type Database struct {
	SqlFormat    string          `xml:"sqlFormat"`
	Roles        *RoleAssignment `xml:"role"`
	ConfigParams []*ConfigParam  `xml:"configurationParameter"`

	// slony
}

type RoleAssignment struct {
	Application string        `xml:"application"`
	Owner       string        `xml:"owner"`
	Replication string        `xml:"replication"`
	ReadOnly    string        `xml:"readonly"`
	CustomRoles DelimitedList `xml:"customRole,omitempty"`
}

type ConfigParam struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

func (db *Database) ToIR() (*ir.Database, error) {
	if db == nil {
		return nil, nil
	}
	rv := ir.Database{
		Roles: &ir.RoleAssignment{
			Application: db.Roles.Application,
			Owner:       db.Roles.Owner,
			Replication: db.Roles.Replication,
			ReadOnly:    db.Roles.ReadOnly,
			CustomRoles: db.Roles.CustomRoles,
		},
	}
	var err error
	rv.SqlFormat, err = ir.NewSqlFormat(db.SqlFormat)
	if err != nil {
		return nil, fmt.Errorf("invalid dababase: %w", err)
	}
	for _, param := range db.ConfigParams {
		rv.ConfigParams = append(
			rv.ConfigParams,
			&ir.ConfigParam{
				Name:  param.Name,
				Value: param.Value,
			},
		)
	}
	return &rv, nil
}
