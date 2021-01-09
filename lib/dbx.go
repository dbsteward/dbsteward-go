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

func (self *DBX) GetTerminalForeignColumn(doc *model.Definition, schema *model.Schema, table *model.Table, column *model.Column) *model.Column {
	local := model.Key{
		Schema:  schema,
		Table:   table,
		Columns: []*model.Column{column},
	}
	foreign := column.TryGetReferencedKey()
	util.Assert(foreign != nil, "GetTerminalForeignColumn called with column that does not reference a foreign column")
	fkey := self.ResolveForeignKey(doc, local, *foreign)
	fcol := fkey.Columns[0]

	if fcol.Type == "" && fcol.ForeignTable != "" {
		GlobalDBSteward.Trace("Seeking nested foreign key for %s", fkey.String())
		return self.GetTerminalForeignColumn(doc, fkey.Schema, fkey.Table, fcol)
	}
	return fcol
}

func (self *DBX) ResolveForeignKey(doc *model.Definition, localKey model.Key, foreignKey model.KeyNames) model.Key {
	fSchema := localKey.Schema
	if foreignKey.Schema != "" {
		fSchema = doc.TryGetSchemaNamed(foreignKey.Schema)
		if fSchema == nil {
			GlobalDBSteward.Fatal("Failed to find foreign schema in %s referenced by %s", foreignKey.String(), localKey.String())
		}
	}

	fTable := fSchema.TryGetTableNamed(foreignKey.Table)
	if fTable == nil {
		GlobalDBSteward.Fatal("Failed to find foreign table in %s referenced by %s", foreignKey.String(), localKey.String())
	}

	// if we didn't ask for specific foreign columns, but we have local columns, use those
	if len(foreignKey.Columns) == 0 {
		util.Assert(len(localKey.Columns) > 0, "Called ResolveForeignKey with no columns to resolve in either localKey or foreignKey")
		foreignKey.Columns = make([]string, len(localKey.Columns))
	}

	if len(localKey.Columns) != len(foreignKey.Columns) {
		GlobalDBSteward.Fatal("Local %s has column count mismatch with foreign %s", localKey.String(), foreignKey.String())
	}

	out := model.Key{
		Schema:  fSchema,
		Table:   fTable,
		Columns: make([]*model.Column, len(foreignKey.Columns)),
		KeyName: foreignKey.KeyName,
	}

	for i, col := range foreignKey.Columns {
		// if the foreign column wasn't specified, use the local column name
		if col == "" {
			util.Assert(localKey.Columns[i] != nil && localKey.Columns[i].Name != "",
				"Called ResolveForeignKey with an empty foreign column but local column name is missing or nil")
			col = localKey.Columns[i].Name
		}

		fCol := fTable.TryGetColumnNamed(col)
		if fCol == nil {
			GlobalDBSteward.Fatal("Failed to find foreign column %s in %s referenced by %s", col, foreignKey.String(), localKey.String())
		}
		out.Columns[i] = fCol
	}

	return out
}

func (self *DBX) EnumRegex(doc *model.Definition) string {
	// TODO(go,core) dbx::enum_regex()
	return ""
}

func (self *DBX) RenamedTableCheckPointer(oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) (*model.Schema, *model.Table) {
	// TODO(go,core) dbx::renamed_table_check_pointer()
	return nil, nil
}
