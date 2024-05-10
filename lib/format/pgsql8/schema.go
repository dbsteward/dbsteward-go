package pgsql8

import (
	"fmt"
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

func (s *Schema) GetCreationSql(dbs *lib.DBSteward, schema *ir.Schema) ([]output.ToSql, error) {
	// don't create the public schema
	if strings.EqualFold(schema.Name, "public") {
		return nil, nil
	}

	ddl := []output.ToSql{
		&sql.SchemaCreate{Schema: schema.Name},
	}

	if schema.Owner != "" {
		owner, err := roleEnum(dbs.Logger(), dbs.NewDatabase, schema.Owner, dbs.IgnoreCustomRoles)
		if err != nil {
			return nil, err
		}
		ddl = append(ddl, &sql.SchemaAlterOwner{Schema: schema.Name, Owner: owner})
	}

	if schema.Description != "" {
		ddl = append(ddl, &sql.SchemaSetComment{Schema: schema.Name, Comment: schema.Description})
	}

	return ddl, nil
}

func (s *Schema) GetDropSql(schema *ir.Schema) []output.ToSql {
	return []output.ToSql{
		&sql.SchemaDrop{
			Schema:  schema.Name,
			Cascade: true,
		},
	}
}

func (s *Schema) GetGrantSql(dbs *lib.DBSteward, doc *ir.Definition, schema *ir.Schema, grant *ir.Grant) ([]output.ToSql, error) {
	roles := make([]string, len(grant.Roles))
	var err error
	for i, role := range grant.Roles {
		roles[i], err = roleEnum(dbs.Logger(), dbs.NewDatabase, role, dbs.IgnoreCustomRoles)
		if err != nil {
			return nil, err
		}
	}

	perms := util.IIntersectStrs(grant.Permissions, ir.PermissionListAllPgsql8)
	invalidPerms := util.IDifferenceStrs(perms, ir.PermissionListValidSchema)
	if len(invalidPerms) > 0 {
		return nil, fmt.Errorf("invalid permissions on schema grant: %v", invalidPerms)
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
	roRole, err := roleEnum(dbs.Logger(), dbs.NewDatabase, ir.RoleReadOnly, dbs.IgnoreCustomRoles)
	if err != nil {
		return nil, err
	}
	if roRole != "" {
		ddl = append(ddl, &sql.SchemaGrant{
			Schema:   schema.Name,
			Perms:    []string{ir.PermissionUsage},
			Roles:    []string{roRole},
			CanGrant: false,
		})
	}

	return ddl, nil
}

func (s *Schema) GetFunctionsDependingOnType(schema *ir.Schema, datatype *ir.TypeDef) []*ir.Function {
	out := []*ir.Function{}
	for _, fn := range schema.Functions {
		if functionDependsOnType(fn, schema, datatype) {
			out = append(out, fn)
		}
	}
	return out
}
