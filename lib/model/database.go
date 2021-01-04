package model

import (
	"github.com/dbsteward/dbsteward/lib/util"
)

type Database struct {
	SqlFormat SqlFormat       `xml:"sqlFormat"`
	Roles     *RoleAssignment `xml:"role"`

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

func (self *Database) Merge(overlay *Database) {
	if overlay == nil {
		return
	}

	self.SqlFormat = overlay.SqlFormat

	if self.Roles == nil {
		self.Roles = &RoleAssignment{}
	}
	self.Roles.Merge(overlay.Roles)
}

func (self *RoleAssignment) IsRoleDefined(role string) bool {
	return util.IIndexOfStr(role, self.CustomRoles) >= 0
}

func (self *RoleAssignment) AddCustomRole(role string) {
	// TODO(feat) sanity check
	self.CustomRoles = append(self.CustomRoles, role)
}

func (self *RoleAssignment) Merge(overlay *RoleAssignment) {
	if overlay == nil {
		return
	}

	self.Application = overlay.Application
	self.Owner = overlay.Owner
	self.Replication = overlay.Replication
	self.ReadOnly = overlay.ReadOnly
	self.CustomRoles = overlay.CustomRoles
}
