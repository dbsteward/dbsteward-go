package pgsql8

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/util"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Schema struct {
}

func NewSchema() *Schema {
	return &Schema{}
}

func (self *Schema) GetCreationSql(schema *ir.Schema) []output.ToSql {
	// don't create the public schema
	if strings.EqualFold(schema.Name, "public") {
		return nil
	}

	ddl := []output.ToSql{
		&sql.SchemaCreate{schema.Name},
	}

	if schema.Owner != "" {
		owner := lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, schema.Owner)
		ddl = append(ddl, &sql.SchemaAlterOwner{schema.Name, owner})
	}

	if schema.Description != "" {
		ddl = append(ddl, &sql.SchemaSetComment{schema.Name, schema.Description})
	}

	return ddl
}

func (self *Schema) GetDropSql(schema *ir.Schema) []output.ToSql {
	return []output.ToSql{
		&sql.SchemaDrop{
			Schema:  schema.Name,
			Cascade: true,
		},
	}
}

func (self *Schema) GetGrantSql(doc *ir.Definition, schema *ir.Schema, grant *ir.Grant) []output.ToSql {
	roles := make([]string, len(grant.Roles))
	for i, role := range grant.Roles {
		roles[i] = lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, role)
	}

	perms := util.IIntersectStrs(grant.Permissions, ir.PermissionListAllPgsql8)
	invalidPerms := util.IDifferenceStrs(perms, ir.PermissionListValidSchema)
	if len(invalidPerms) > 0 {
		lib.GlobalDBSteward.Fatal("Invalid permissions on schema grant: %v", invalidPerms)
	}

	ddl := []output.ToSql{
		&sql.SchemaGrant{
			Schema:   schema.Name,
			Perms:    []string(perms),
			Roles:    roles,
			CanGrant: grant.CanGrant(),
		},
	}

	// SCHEMA IMPLICIT GRANTS
	// READYONLY USER PROVISION: grant usage on the schema for the readonly user
	// TODO(go,3) move this out of here, let this create just a single grant
	roRole := lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, ir.RoleReadOnly)
	if roRole != "" {
		ddl = append(ddl, &sql.SchemaGrant{
			Schema:   schema.Name,
			Perms:    []string{ir.PermissionUsage},
			Roles:    []string{roRole},
			CanGrant: false,
		})
	}

	return ddl
}

func (self *Schema) GetFunctionsDependingOnType(schema *ir.Schema, datatype *ir.DataType) []*ir.Function {
	out := []*ir.Function{}
	for _, fn := range schema.Functions {
		if functionDependsOnType(fn, schema, datatype) {
			out = append(out, fn)
		}
	}
	return out
}
