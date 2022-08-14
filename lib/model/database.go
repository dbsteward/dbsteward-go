package model

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
)

type Database struct {
	SqlFormat    SqlFormat
	Roles        *RoleAssignment
	ConfigParams []*ConfigParam

	// slony
}

type RoleAssignment struct {
	Application string
	Owner       string
	Replication string
	ReadOnly    string
	CustomRoles []string
}

type ConfigParam struct {
	Name  string
	Value string
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

func (self *Database) TryGetConfigParamNamed(name string) *ConfigParam {
	for _, param := range self.ConfigParams {
		if strings.EqualFold(param.Name, name) {
			return param
		}
	}
	return nil
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
	return util.IStrsContains(self.CustomRoles, role)
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

func (self *ConfigParam) Equals(other *ConfigParam) bool {
	if self == nil || other == nil {
		return false
	}
	return self.Value != other.Value
}
