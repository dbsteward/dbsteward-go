package xml

import (
	"fmt"
	"log/slog"

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

func RoleAssignmentFromIR(l *slog.Logger, i *ir.RoleAssignment) *RoleAssignment {
	l.Debug("translating role assignments")
	defer l.Debug("done translating role assignments")
	ra := RoleAssignment{
		Application: i.Application,
		Owner:       i.Owner,
		Replication: i.Replication,
		ReadOnly:    i.ReadOnly,
		CustomRoles: i.CustomRoles,
	}
	return &ra
}

type ConfigParam struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

func ConfigParamsFromIR(l *slog.Logger, c []*ir.ConfigParam) []*ConfigParam {
	l.Debug("translating config parameters")
	defer l.Debug("done translating config parameters")
	if len(c) == 0 {
		return nil
	}
	var cp []*ConfigParam
	for _, ircp := range c {
		cp = append(
			cp,
			&ConfigParam{
				Name:  ircp.Name,
				Value: ircp.Value,
			},
		)
	}
	return cp
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
