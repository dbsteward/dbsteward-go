package pgsql8

import (
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
				ident := GlobalOperations.IdentifierName(schema.Name, table.Name, col.Name, "_seq")
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
