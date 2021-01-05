package pgsql8

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

var GlobalColumn *Column = NewColumn()

func NewColumn() *Column {
	return &Column{}
}

type Column struct {
}

func (self *Column) GetReducedDefinition(doc *model.Definition, schema *model.Schema, table *model.Table, column *model.Column) sql.CreateTableColumn {
	return sql.CreateTableColumn{
		Column: column.Name,
		Type:   self.GetColumnType(doc, schema, table, column),
	}
}

func (self *Column) GetSetupSql(schema *model.Schema, table *model.Table, column *model.Column) []output.ToSql {
	ddl := []output.ToSql{}
	colref := sql.ColumnRef{schema.Name, table.Name, column.Name}
	if column.Statistics != nil {
		ddl = append(ddl, &sql.AlterColumnStatistics{
			Column:     colref,
			Statistics: *column.Statistics,
		})
	}
	if column.Description != "" {
		ddl = append(ddl, &sql.SetColumnComment{
			Column:  colref,
			Comment: column.Description,
		})
	}

	return ddl
}

func (self *Column) IsSerialType(column *model.Column) bool {
	return util.IIndexOfStr(column.Type, []string{DataTypeSerial, DataTypeBigSerial}) >= 0
}

func (self *Column) GetColumnType(doc *model.Definition, schema *model.Schema, table *model.Table, column *model.Column) string {
	// if it is a foreign keyed column, solve for the foreign key type
	if column.ForeignTable != "" {
		// TODO(feat) what about compound FKs?
		foreign := lib.GlobalDBX.GetTerminalForeignColumn(doc, schema, table, column)
		return self.GetReferenceType(foreign.Type)
	}

	if column.Type == "" {
		// TODO(go,nth) is this already checked ahead of time? can it be?
		lib.GlobalDBSteward.Fatal("column missing type -- %s.%s.%s", schema.Name, table.Name, column.Name)
	}

	// TODO(go,pgsql) need to indicate to the Quoter that we _must_ quote this value.
	// I have no idea how to do that off the top of my head

	// if lib.GlobalDBX.GetType(schema, column.Type) != nil {
	// 	// this is a user defined type or enum, enforce quoting if set
	// 	return GlobalOperations.GetQuotedObjectName(column.Type)
	// }

	return column.Type
}

// GetReferenceType returns the data type needed to reference a column of the given type
// e.g. GetReferenceType("serial") == "int"
func (self *Column) GetReferenceType(coltype string) string {
	if strings.EqualFold(coltype, DataTypeSerial) {
		return DataTypeInt
	}
	if strings.EqualFold(coltype, DataTypeBigSerial) {
		return DataTypeBigInt
	}
	// TODO(feat) should this include enum types?
	return coltype
}
