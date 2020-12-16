package model

import (
	"github.com/dbsteward/dbsteward/lib/format"
	"github.com/dbsteward/dbsteward/lib/util"
)

type Database struct {
	SqlFormat format.SqlFormat `xml:"sqlFormat"`
	Roles     *RoleAssignment  `xml:"role"`

	// slony, configurationParameter
}

type RoleAssignment struct {
	Application string        `xml:"application"`
	Owner       string        `xml:"owner"`
	Replication string        `xml:"replication"`
	ReadOnly    string        `xml:"readonly"`
	CustomRoles DelimitedList `xml:"customRole"`
}

func (self *Database) IsRoleDefined(role string) bool {
	if self.Roles == nil {
		return false
	}
	return self.Roles.IsRoleDefined(role)
}

func (self *Database) AddCustomRole(role string) {
	if self.Roles == nil {
		self.Roles = &RoleAssignment{}
	}
	self.Roles.AddCustomRole(role)
}

func (self *RoleAssignment) IsRoleDefined(role string) bool {
	return util.IIndexOfStr(role, self.CustomRoles) >= 0
}

func (self *RoleAssignment) AddCustomRole(role string) {
	// TODO(feat) sanity check
	self.CustomRoles = append(self.CustomRoles, role)
}
