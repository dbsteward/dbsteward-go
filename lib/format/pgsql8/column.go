package pgsql8

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

func getReducedColumnDefinition(doc *ir.Definition, schema *ir.Schema, table *ir.Table, column *ir.Column) sql.ColumnDefinition {
	return sql.ColumnDefinition{
		Name: column.Name,
		Type: sql.ParseTypeRef(getColumnType(doc, schema, table, column)),
	}
}

func getFullColumnDefinition(doc *ir.Definition, schema *ir.Schema, table *ir.Table, column *ir.Column, includeNullDefinition, includeDefaultNextval bool) sql.ColumnDefinition {
	colType := getColumnType(doc, schema, table, column)
	out := sql.ColumnDefinition{
		Name:     column.Name,
		Type:     sql.ParseTypeRef(colType),
		Default:  nil,
		Nullable: nil,
	}

	if column.Default != "" {
		if !includeDefaultNextval && hasDefaultNextval(column) {
			// if the default is a nextval expression, don't specify it in the regular full definition
			// because if the sequence has not been defined yet,
			// the nextval expression will be evaluated inline and fail
			lib.GlobalDBSteward.Info(
				"Skipping %s.%s default expression \"%s\" - this default expression will be applied after all sequences have been created",
				table.Name,
				column.Name,
				column.Default,
			)
		} else {
			deftmp := sql.RawSql(column.Default)
			out.Default = &deftmp
		}
	}

	if includeNullDefinition {
		nulltmp := column.Nullable
		out.Nullable = &nulltmp
	}

	return out
}

func getColumnSetupSql(schema *ir.Schema, table *ir.Table, column *ir.Column) []output.ToSql {
	ddl := []output.ToSql{}
	colref := sql.ColumnRef{Schema: schema.Name, Table: table.Name, Column: column.Name}
	if column.Statistics != nil {
		ddl = append(ddl, &sql.ColumnAlterStatistics{
			Column:     colref,
			Statistics: *column.Statistics,
		})
	}
	if column.Description != "" {
		ddl = append(ddl, &sql.ColumnSetComment{
			Column:  colref,
			Comment: column.Description,
		})
	}

	return ddl
}

func getColumnDefaultSql(schema *ir.Schema, table *ir.Table, column *ir.Column) []output.ToSql {
	if !includeColumnDefaultNextvalInCreateSql && hasDefaultNextval(column) {
		// if the default is a nextval expression, don't specify it in the regular full definition
		// because if the sequence has not been defined yet,
		// the nextval expression will be evaluated inline and fail
		lib.GlobalDBSteward.Info(
			"Skipping %s.%s.%s default expression \"%s\" - this default expression will be applied after all sequences have been created",
			schema.Name,
			table.Name,
			column.Name,
			column.Default,
		)
		return nil
	}
	ref := sql.ColumnRef{Schema: schema.Name, Table: table.Name, Column: column.Name}
	out := []output.ToSql{}

	if column.Default != "" {
		out = append(out, &sql.ColumnSetDefault{
			Column:  ref,
			Default: sql.RawSql(column.Default),
		})
	}

	if !column.Nullable {
		out = append(out, &sql.ColumnSetNull{
			Column:   ref,
			Nullable: false,
		})
	}

	return out
}

func isSerialType(column *ir.Column) bool {
	return GlobalDataType.IsSerialType(column.Type)
}

func hasDefaultNextval(column *ir.Column) bool {
	if column.Default != "" {
		return len(util.IMatch(`^nextval\((.+)\)$`, column.Default)) > 0
	}
	return false
}

func hasDefaultNow(column *ir.Column) bool {
	// TODO(feat) what about expressions with now/current_timestamp?
	return strings.EqualFold(column.Default, "now()") || strings.EqualFold(column.Default, "current_timestamp")
}

// TODO(go,3) it would be super if types had dedicated types/values
func getColumnType(doc *ir.Definition, schema *ir.Schema, table *ir.Table, column *ir.Column) string {
	// if it is a foreign keyed column, solve for the foreign key type
	if column.ForeignTable != "" {
		// TODO(feat) what about compound FKs?
		foreign := lib.GlobalDBX.GetTerminalForeignColumn(doc, schema, table, column)
		return getReferenceType(foreign.Type)
	}

	if column.Type == "" {
		lib.GlobalDBSteward.Fatal("column %s.%s.%s missing type", schema.Name, table.Name, column.Name)
	}

	if schema.TryGetTypeNamed(column.Type) != nil {
		// this is a user defined type in the same schema, make sure to qualify it for later
		// TODO(go,3) what if it's in a different schema?
		return schema.Name + "." + column.Type
	}

	return column.Type
}

// GetReferenceType returns the data type needed to reference a column of the given type
// e.g. GetReferenceType("serial") == "int"
func getReferenceType(coltype string) string {
	if strings.EqualFold(coltype, DataTypeSerial) {
		return DataTypeInt
	}
	if strings.EqualFold(coltype, DataTypeBigSerial) {
		return DataTypeBigInt
	}
	// TODO(feat) should this include enum types?
	return coltype
}
