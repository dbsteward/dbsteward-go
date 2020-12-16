package model

import (
	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/pkg/errors"
)

// TODO(go,3) move most public fields to private, use accessors to better enable encapsulation, validation; "make invalid states unrepresentable"

type Definition struct {
	Database  *Database   `xml:"database"`
	Schemas   []*Schema   `xml:"schema"`
	Languages []*Language `xml:"language"`
}

func (self *Definition) GetSchemaNamed(name string) (*Schema, error) {
	matching := []*Schema{}
	for _, schema := range self.Schemas {
		// TODO(feat) case insensitivity?
		if schema.Name == name {
			matching = append(matching, schema)
		}
	}
	if len(matching) == 0 {
		return nil, errors.Errorf("no schema named %s found", name)
	}
	if len(matching) > 1 {
		return nil, errors.Errorf("more than one schema named %s found", name)
	}
	return matching[0], nil
}

func (self *Definition) TryGetSchemaNamed(name string) *Schema {
	for _, schema := range self.Schemas {
		// TODO(feat) case insensitivity?
		if schema.Name == name {
			return schema
		}
	}
	return nil
}

func (self *Definition) AddSchema(schema *Schema) {
	// TODO(feat) sanity check duplicate name
	self.Schemas = append(self.Schemas, schema)
}

func (self *Definition) IsRoleDefined(role string) bool {
	if util.IIndexOfStr(role, MACRO_ROLES) >= 0 {
		return true
	}
	if self.Database == nil {
		return false
	}
	return self.Database.IsRoleDefined(role)
}

func (self *Definition) AddCustomRole(role string) {
	if self.Database == nil {
		// TODO(go,nth) incomplete construction
		self.Database = &Database{}
	}
	self.Database.AddCustomRole(role)
}
