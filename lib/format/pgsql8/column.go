package pgsql8

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

func getReducedColumnDefinition(l *slog.Logger, doc *ir.Definition, schema *ir.Schema, table *ir.Table, column *ir.Column) (sql.ColumnDefinition, error) {
	t, err := getColumnType(l, doc, schema, table, column)
	if err != nil {
		return sql.ColumnDefinition{}, err
	}
	return sql.ColumnDefinition{
		Name: column.Name,
		Type: sql.ParseTypeRef(t),
	}, nil
}

func getFullColumnDefinition(l *slog.Logger, doc *ir.Definition, schema *ir.Schema, table *ir.Table, column *ir.Column, includeNullDefinition, includeDefaultNextval bool) (sql.ColumnDefinition, error) {
	colType, err := getColumnType(l, doc, schema, table, column)
	if err != nil {
		return sql.ColumnDefinition{}, err
	}
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
			l.Info(fmt.Sprintf(
				"Skipping %s.%s default expression \"%s\" - this default expression will be applied after all sequences have been created",
				table.Name,
				column.Name,
				column.Default,
			))
		} else {
			deftmp := sql.RawSql(column.Default)
			out.Default = &deftmp
		}
	}

	if includeNullDefinition {
		nulltmp := column.Nullable
		out.Nullable = &nulltmp
	}

	return out, nil
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

func getColumnDefaultSql(l *slog.Logger, schema *ir.Schema, table *ir.Table, column *ir.Column) []output.ToSql {
	if !includeColumnDefaultNextvalInCreateSql && hasDefaultNextval(column) {
		// if the default is a nextval expression, don't specify it in the regular full definition
		// because if the sequence has not been defined yet,
		// the nextval expression will be evaluated inline and fail
		l.Info(fmt.Sprintf(
			"Skipping %s.%s.%s default expression \"%s\" - this default expression will be applied after all sequences have been created",
			schema.Name,
			table.Name,
			column.Name,
			column.Default,
		))
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

func isColumnSerialType(column *ir.Column) bool {
	return isSerialType(column.Type)
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
func getColumnType(l *slog.Logger, doc *ir.Definition, schema *ir.Schema, table *ir.Table, column *ir.Column) (string, error) {
	// if it is a foreign keyed column, solve for the foreign key type
	if column.ForeignTable != "" {
		// TODO(feat) what about compound FKs?
		foreign, err := doc.GetTerminalForeignColumn(l, schema, table, column)
		if err != nil {
			return "", err
		}
		return getReferenceType(foreign.Type), nil
	}

	if column.Type == "" {
		return "", fmt.Errorf("column %s.%s.%s missing type", schema.Name, table.Name, column.Name)
	}

	if schema.TryGetTypeNamed(column.Type) != nil {
		// this is a user defined type in the same schema, make sure to qualify it for later
		// TODO(go,3) what if it's in a different schema?
		return schema.Name + "." + column.Type, nil
	}

	return column.Type, nil
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
