package pgsql8

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

var GlobalTable *Table = NewTable()

type Table struct {
	IncludeColumnDefaultNextvalInCreateSql bool
}

func NewTable() *Table {
	return &Table{}
}

func (self *Table) GetCreationSql(schema *model.Schema, table *model.Table) []output.ToSql {
	GlobalOperations.SetContextReplicaSetId(table.SlonySetId)

	cols := []sql.TableCreateColumn{}
	colSetup := []output.ToSql{}
	for _, col := range table.Columns {
		cols = append(cols, GlobalColumn.GetReducedDefinition(lib.GlobalDBSteward.NewDatabase, schema, table, col))
		colSetup = append(colSetup, GlobalColumn.GetSetupSql(schema, table, col)...)
	}

	opts := []sql.TableCreateOption{}
	for _, opt := range table.TableOptions {
		if opt.SqlFormat == model.SqlFormatPgsql8 {
			opts = append(opts, sql.TableCreateOption{opt.Name, opt.Value})
		}
	}

	ddl := []output.ToSql{
		&sql.TableCreate{
			Table:        sql.TableRef{schema.Name, table.Name},
			Columns:      cols,
			OtherOptions: opts,
		},
	}

	if table.Description != "" {
		ddl = append(ddl, &sql.TableSetComment{
			Table:   sql.TableRef{schema.Name, table.Name},
			Comment: table.Description,
		})
	}

	ddl = append(ddl, colSetup...)

	if table.Owner != "" {
		role := lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, table.Owner)
		ddl = append(ddl, &sql.TableAlterOwner{
			Table: sql.TableRef{schema.Name, table.Name},
			Role:  role,
		})

		// update the owner of all linked tables as well
		for _, col := range table.Columns {
			// TODO(feat) more than just serials?
			if GlobalColumn.IsSerialType(col) {
				ident := GlobalOperations.BuildSequenceName(schema.Name, table.Name, col.Name)
				ddl = append(ddl, &sql.TableAlterOwner{
					Table: sql.TableRef{schema.Name, ident},
					Role:  role,
				})
			}
		}
	}

	return ddl
}

func (self *Table) GetDefaultNextvalSql(schema *model.Schema, table *model.Table) []output.ToSql {
	// TODO(go,pgsql)
	return nil
}

func (self *Table) DefineTableColumnDefaults(schema *model.Schema, table *model.Table) []output.ToSql {
	// TODO(go,pgsql)
	return nil
}

func (self *Table) GetGrantSql(doc *model.Definition, schema *model.Schema, table *model.Table, grant *model.Grant) []output.ToSql {
	GlobalOperations.SetContextReplicaSetId(table.SlonySetId)

	roles := make([]string, len(grant.Roles))
	for i, role := range grant.Roles {
		roles[i] = lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, role)
	}

	perms := util.IIntersectStrs(grant.Permissions, model.PermissionListAllPgsql8)
	if len(perms) == 0 {
		lib.GlobalDBSteward.Fatal("No format-compatible permissions on table %s.%s grant: %v", schema.Name, table.Name, grant.Permissions)
	}
	invalidPerms := util.IDifferenceStrs(perms, model.PermissionListValidTable)
	if len(invalidPerms) > 0 {
		lib.GlobalDBSteward.Fatal("Invalid permissions on table %s.%s grant: %v", schema.Name, table.Name, invalidPerms)
	}

	ddl := []output.ToSql{
		&sql.TableGrant{
			Table: sql.TableRef{schema.Name, table.Name},
			Perms: []string(grant.Permissions),
			Roles: roles,
		},
	}

	// TABLE IMPLICIT GRANTS
	// READYONLY USER PROVISION: grant select on the table for the readonly user
	// TODO(go,3) move this out of here, let this create just a single grant
	roRole := lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, model.RoleReadOnly)
	if roRole != "" {
		ddl = append(ddl, &sql.TableGrant{
			Table:    sql.TableRef{schema.Name, table.Name},
			Perms:    []string{model.PermissionSelect},
			Roles:    []string{roRole},
			CanGrant: false,
		})
	}

	// don't need to grant cascaded serial permissions to the table owner
	rolesNotOwner := []string{}
	for _, role := range grant.Roles {
		if !strings.EqualFold(role, model.RoleOwner) {
			rolesNotOwner = append(rolesNotOwner, role)
		}
	}

	// set serial columns permissions based on table permissions
	for _, column := range table.Columns {
		if !GlobalColumn.IsSerialType(column) {
			continue
		}

		// if you can SELECT, INSERT or UPDATE the table, you can SELECT on the sequence
		// if you can INSERT or UPDATE the table, you can UPDATE the sequence
		seqPerms := []string{}
		updatePerms := []string{model.PermissionInsert, model.PermissionUpdate}
		selectPerms := append(updatePerms, model.PermissionSelect)
		if len(util.IIntersectStrs(selectPerms, grant.Permissions)) > 0 {
			seqPerms = append(seqPerms, model.PermissionSelect)
		}
		if len(util.IIntersectStrs(updatePerms, grant.Permissions)) > 0 {
			seqPerms = append(seqPerms, model.PermissionUpdate)
		}

		seqRef := sql.SequenceRef{
			Schema:   schema.Name,
			Sequence: GlobalOperations.BuildSequenceName(schema.Name, table.Name, column.Name),
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
				Perms:    []string{model.PermissionSelect}, // TODO(feat) doesn't this need to have usage too?
				Roles:    []string{roRole},
				CanGrant: false,
			})
		}
	}

	return ddl
}
