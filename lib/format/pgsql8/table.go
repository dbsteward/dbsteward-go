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

var includeColumnDefaultNextvalInCreateSql bool

func getCreateTableSql(schema *ir.Schema, table *ir.Table) []output.ToSql {
	cols := []sql.ColumnDefinition{}
	colSetup := []output.ToSql{}
	for _, col := range table.Columns {
		cols = append(cols, getReducedColumnDefinition(lib.GlobalDBSteward.NewDatabase, schema, table, col))
		colSetup = append(colSetup, getColumnSetupSql(schema, table, col)...)
	}

	opts := []sql.TableCreateOption{}
	for _, opt := range table.TableOptions {
		if opt.SqlFormat == ir.SqlFormatPgsql8 {
			opts = append(opts, sql.TableCreateOption{Option: opt.Name, Value: opt.Value})
		}
	}

	var inherits *sql.TableRef
	if table.InheritsTable != "" {
		// TODO(go,nth) validate
		inherits = &sql.TableRef{
			Schema: util.CoalesceStr(table.InheritsSchema, schema.Name),
			Table:  table.InheritsTable,
		}
	}

	ddl := []output.ToSql{
		&sql.TableCreate{
			Table:        sql.TableRef{Schema: schema.Name, Table: table.Name},
			Columns:      cols,
			Inherits:     inherits,
			OtherOptions: opts,
		},
	}

	if table.Description != "" {
		ddl = append(ddl, &sql.TableSetComment{
			Table:   sql.TableRef{Schema: schema.Name, Table: table.Name},
			Comment: table.Description,
		})
	}

	ddl = append(ddl, colSetup...)

	if table.Owner != "" {
		role := lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, table.Owner)
		ddl = append(ddl, &sql.TableAlterOwner{
			Table: sql.TableRef{Schema: schema.Name, Table: table.Name},
			Role:  role,
		})

		// update the owner of all linked tables as well
		for _, col := range table.Columns {
			// TODO(feat) more than just serials?
			if isSerialType(col) {
				ident := buildSequenceName(schema.Name, table.Name, col.Name)
				ddl = append(ddl, &sql.TableAlterOwner{
					Table: sql.TableRef{Schema: schema.Name, Table: ident},
					Role:  role,
				})
			}
		}
	}

	return ddl
}

func getDropTableSql(schema *ir.Schema, table *ir.Table) []output.ToSql {
	return []output.ToSql{
		&sql.TableDrop{
			Table: sql.TableRef{Schema: schema.Name, Table: table.Name},
		},
	}
}

func getDefaultNextvalSql(schema *ir.Schema, table *ir.Table) []output.ToSql {
	out := []output.ToSql{}
	for _, column := range table.Columns {
		if hasDefaultNextval(column) {
			lib.GlobalDBSteward.Info("Specifying skipped %s.%s.%s default expression \"%s\"", schema.Name, table.Name, column.Name, column.Default)
			out = append(out, &sql.Annotated{
				Wrapped: &sql.ColumnSetDefault{
					Column:  sql.ColumnRef{Schema: schema.Name, Table: table.Name, Column: column.Name},
					Default: sql.RawSql(column.Default),
				},
				Annotation: "column default nextval expression being added post table creation",
			})
		}
	}
	return out
}

func defineTableColumnDefaults(schema *ir.Schema, table *ir.Table) []output.ToSql {
	out := []output.ToSql{}
	for _, column := range table.Columns {
		out = append(out, getColumnDefaultSql(schema, table, column)...)
	}
	return out
}

func getTableGrantSql(schema *ir.Schema, table *ir.Table, grant *ir.Grant) []output.ToSql {
	roles := make([]string, len(grant.Roles))
	for i, role := range grant.Roles {
		roles[i] = lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, role)
	}

	perms := util.IIntersectStrs(grant.Permissions, ir.PermissionListAllPgsql8)
	if len(perms) == 0 {
		lib.GlobalDBSteward.Fatal("No format-compatible permissions on table %s.%s grant: %v", schema.Name, table.Name, grant.Permissions)
	}
	invalidPerms := util.IDifferenceStrs(perms, ir.PermissionListValidTable)
	if len(invalidPerms) > 0 {
		lib.GlobalDBSteward.Fatal("Invalid permissions on table %s.%s grant: %v", schema.Name, table.Name, invalidPerms)
	}

	ddl := []output.ToSql{
		&sql.TableGrant{
			Table: sql.TableRef{Schema: schema.Name, Table: table.Name},
			Perms: []string(grant.Permissions),
			Roles: roles,
		},
	}

	// TABLE IMPLICIT GRANTS
	// READYONLY USER PROVISION: grant select on the table for the readonly user
	// TODO(go,3) move this out of here, let this create just a single grant
	roRole := lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, ir.RoleReadOnly)
	if roRole != "" {
		ddl = append(ddl, &sql.TableGrant{
			Table:    sql.TableRef{Schema: schema.Name, Table: table.Name},
			Perms:    []string{ir.PermissionSelect},
			Roles:    []string{roRole},
			CanGrant: false,
		})
	}

	// don't need to grant cascaded serial permissions to the table owner
	rolesNotOwner := []string{}
	for _, role := range roles {
		if !strings.EqualFold(role, ir.RoleOwner) {
			rolesNotOwner = append(rolesNotOwner, role)
		}
	}

	// set serial columns permissions based on table permissions
	for _, column := range table.Columns {
		if !isSerialType(column) {
			continue
		}

		// if you can SELECT, INSERT or UPDATE the table, you can SELECT on the sequence
		// if you can INSERT or UPDATE the table, you can UPDATE the sequence
		seqPerms := []string{}
		updatePerms := []string{ir.PermissionInsert, ir.PermissionUpdate}
		selectPerms := append(updatePerms, ir.PermissionSelect)
		if len(util.IIntersectStrs(selectPerms, grant.Permissions)) > 0 {
			seqPerms = append(seqPerms, ir.PermissionSelect)
		}
		if len(util.IIntersectStrs(updatePerms, grant.Permissions)) > 0 {
			seqPerms = append(seqPerms, ir.PermissionUpdate)
		}

		seqRef := sql.SequenceRef{
			Schema:   schema.Name,
			Sequence: buildSequenceName(schema.Name, table.Name, column.Name),
		}
		if len(seqPerms) > 0 {
			ddl = append(ddl, &sql.SequenceGrant{
				Sequence: seqRef,
				Perms:    seqPerms,
				Roles:    rolesNotOwner,
				CanGrant: grant.CanGrant(),
			})
		}

		// READYONLY USER PROVISION: grant implicit select on the sequence for the readonly user
		if roRole != "" {
			ddl = append(ddl, &sql.SequenceGrant{
				Sequence: seqRef,
				Perms:    []string{ir.PermissionSelect}, // TODO(feat) doesn't this need to have usage too?
				Roles:    []string{roRole},
				CanGrant: false,
			})
		}
	}

	return ddl
}

func getSerialStartDml(schema *ir.Schema, table *ir.Table, column *ir.Column) []output.ToSql {
	if column == nil {
		out := []output.ToSql{}
		for _, column := range table.Columns {
			out = append(out, getSerialStartDml(schema, table, column)...)
		}
		return out
	}
	return _getSerialStartDml(schema, table, column)
}

func _getSerialStartDml(schema *ir.Schema, table *ir.Table, column *ir.Column) []output.ToSql {
	if column.SerialStart == nil {
		return nil
	}
	if !isSerialType(column) {
		lib.GlobalDBSteward.Fatal("Expected serial type for column %s.%s.%s because serialStart='%d' was defined, found type %s",
			schema.Name, table.Name, column.Name, *column.SerialStart, column.Type)
	}
	return []output.ToSql{
		&sql.Annotated{
			Annotation: fmt.Sprintf("serialStart %d specified for %s.%s.%s", *column.SerialStart, schema.Name, table.Name, column.Name),
			Wrapped: &sql.SequenceSerialSetVal{
				Column: sql.ColumnRef{Schema: schema.Name, Table: table.Name, Column: column.Name},
				Value:  *column.SerialStart,
			},
		},
	}
}

func parseStorageParams(value string) map[string]string {
	return util.ParseKV(value[1:len(value)-1], ",", "=")
}
