package lib

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

var GlobalDBX *DBX = NewDBX()

type DBX struct {
	defaultSchema *model.Schema
}

func NewDBX() *DBX {
	return &DBX{}
}

func (self *DBX) SetDefaultSchema(def *model.Definition, schema string) *model.Schema {
	self.defaultSchema = def.GetOrCreateSchemaNamed(schema)
	return self.defaultSchema
}
func (self *DBX) GetDefaultSchema() *model.Schema {
	return self.defaultSchema
}

func (self *DBX) BuildStagedSql(doc *model.Definition, ofs output.OutputFileSegmenter, stage string) {
	// TODO(go,core) dbx::build_staged_sql()
}

// TODO(feat) what about compound keys?
func (self *DBX) GetTerminalForeignColumn(doc *model.Definition, schema *model.Schema, table *model.Table, column *model.Column) *model.Column {
	fSchemaName := util.CoalesceStr(column.ForeignSchema, schema.Name)
	fSchema, err := doc.GetSchemaNamed(fSchemaName)
	GlobalDBSteward.FatalIfError(err, "Failed to find foreign schema '%s' for %s.%s.%s", fSchemaName, schema.Name, table.Name, column.Name)

	fTable, err := fSchema.GetTableNamed(column.ForeignTable)
	GlobalDBSteward.FatalIfError(err, "Failed to find foreign table '%s' for %s.%s.%s", column.ForeignTable, schema.Name, table.Name, column.Name)

	fColumnName := util.CoalesceStr(column.ForeignColumn, column.Name)
	fColumn, err := fTable.GetColumnNamed(fColumnName)
	GlobalDBSteward.FatalIfError(err, "Failed to find foreign column '%s' on foreign table '%s.%s' for %s.%s.%s", fColumnName, fSchema.Name, fTable.Name, schema.Name, table.Name, column.Name)

	if fColumn.Type == "" && fColumn.ForeignColumn != "" {
		GlobalDBSteward.Trace("Seeking nested foreign key for %s.%s.%s", fColumn.Name, fTable.Name, fSchema.Name)
		return self.GetTerminalForeignColumn(doc, fSchema, fTable, fColumn)
	}
	return fColumn
}

func (self *DBX) EnumRegex(doc *model.Definition) string {
	// TODO(go,core) dbx::enum_regex()
	return ""
}

func (self *DBX) RenamedTableCheckPointer(oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) (*model.Schema, *model.Table) {
	// TODO(go,core) dbx::renamed_table_check_pointer()
	return nil, nil
}
