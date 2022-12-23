package lib

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/pkg/errors"
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

func (self *DBX) BuildStagedSql(doc *model.Definition, ofs output.OutputFileSegmenter, stage model.SqlStage) {
	if stage == "" {
		ofs.Write("\n-- NON-STAGED SQL COMMANDS\n")
	} else {
		ofs.Write("\n-- SQL STAGE %s COMMANDS\n", stage)
	}
	for _, sql := range doc.Sql {
		GlobalDBSteward.Lookup().Operations.SetContextReplicaSetId(sql.SlonySetId)
		if sql.Stage.Equals(stage) {
			if sql.Comment != "" {
				ofs.Write("%s\n", util.PrefixLines(sql.Comment, "-- "))
			}
			ofs.Write("%s\n", strings.TrimSpace(sql.Text))
		}
	}
	ofs.Write("\n")
}

func (self *DBX) GetTerminalForeignColumn(doc *model.Definition, schema *model.Schema, table *model.Table, column *model.Column) *model.Column {
	fkey := self.ResolveForeignKeyColumn(doc, schema, table, column)
	fcol := fkey.Columns[0]

	if fcol.Type == "" && fcol.ForeignTable != "" {
		GlobalDBSteward.Trace("Seeking nested foreign key for %s", fkey.String())
		return self.GetTerminalForeignColumn(doc, fkey.Schema, fkey.Table, fcol)
	}
	return fcol
}

func (self *DBX) ResolveForeignKeyColumn(doc *model.Definition, schema *model.Schema, table *model.Table, column *model.Column) model.Key {
	// this used to be called format_constraint::foreign_key_lookup() in v1
	// most of the functionality got split to the more general ResolveForeignKey
	foreign := column.TryGetReferencedKey()
	util.Assert(foreign != nil, "ResolveForeignKeyColumn called with column that does not reference a foreign column")

	local := model.Key{
		Schema:  schema,
		Table:   table,
		Columns: []*model.Column{column},
	}
	return self.ResolveForeignKey(doc, local, *foreign)
}

func (self *DBX) ResolveForeignKey(doc *model.Definition, localKey model.Key, foreignKey model.KeyNames) model.Key {
	fref := self.ResolveSchemaTable(doc, localKey.Schema, foreignKey.Schema, foreignKey.Table, "foreign key")

	// if we didn't ask for specific foreign columns, but we have local columns, use those
	if len(foreignKey.Columns) == 0 {
		util.Assert(len(localKey.Columns) > 0, "Called ResolveForeignKey with no columns to resolve in either localKey or foreignKey")
		foreignKey.Columns = make([]string, len(localKey.Columns))
	}

	if len(localKey.Columns) != len(foreignKey.Columns) {
		GlobalDBSteward.Fatal("Local %s has column count mismatch with foreign %s", localKey.String(), foreignKey.String())
	}

	out := model.Key{
		Schema:  fref.Schema,
		Table:   fref.Table,
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

		fCol := self.TryInheritanceGetColumn(doc, fref.Schema, fref.Table, col)
		if fCol == nil {
			GlobalDBSteward.Fatal("Failed to find foreign column %s in %s referenced by %s", col, foreignKey.String(), localKey.String())
		}
		out.Columns[i] = fCol
	}

	return out
}

func (self *DBX) ResolveSchemaTable(doc *model.Definition, localSchema *model.Schema, schemaName, tableName string, refType string) model.TableRef {
	fSchema := localSchema
	if schemaName != "" {
		fSchema = doc.TryGetSchemaNamed(schemaName)
		if fSchema == nil {
			GlobalDBSteward.Fatal("%s reference to unknown schema %s", refType, schemaName)
		}
	}

	fTable := fSchema.TryGetTableNamed(tableName)
	if fTable == nil {
		GlobalDBSteward.Fatal("%s reference to unknown table %s.%s", refType, fSchema.Name, tableName)
	}

	return model.TableRef{fSchema, fTable}
}

// attempts to find the new table that claims it is renamed from the old table
// this is the "forwards looking" version of RenamedTableCheckPointer
func (self *DBX) TryGetTableFormerlyKnownAs(newDoc *model.Definition, oldSchema *model.Schema, oldTable *model.Table) *model.TableRef {
	// TODO(go,nth) can we remove the assertion in favor of just returning nil? or should callers continue to check IgnoreOldNames themselves?
	util.Assert(!GlobalDBSteward.IgnoreOldNames, "Should not attempt to look up renamed tables if IgnoreOldNames is set")

	// TODO(go,3) move to model, and/or compositing pass
	for _, newSchema := range newDoc.Schemas {
		for _, newTable := range newSchema.Tables {
			if newTable.OldTableName != "" || newTable.OldSchemaName != "" {
				oldTableName := util.CoalesceStr(newTable.OldTableName, newTable.Name)
				oldSchemaName := util.CoalesceStr(newTable.OldSchemaName, newSchema.Name)
				if strings.EqualFold(oldSchema.Name, oldSchemaName) && strings.EqualFold(oldTable.Name, oldTableName) {
					return &model.TableRef{newSchema, newTable}
				}
			}
		}
	}
	return nil
}

// attempts to find, and sanity checks, the table pointed to by oldSchema/TableName attributes
// this is the "backwards looking" version of TryGetTableFormerlyKnownAs
// TODO(go,nth) rename this, clean it up
func (self *DBX) RenamedTableCheckPointer(oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) (*model.Schema, *model.Table) {
	if newSchema == nil || newTable == nil {
		return oldSchema, oldTable
	}

	isRenamed, err := self.IsRenamedTable(newSchema, newTable)
	GlobalDBSteward.FatalIfError(err, "while checking table rename status")
	if !isRenamed {
		return oldSchema, oldTable
	}

	if newTable.OldSchemaName != "" {
		oldSchema = self.GetOldTableSchema(newSchema, newTable)
		if oldSchema == nil {
			GlobalDBSteward.Fatal("Sanity failure: %s.%s has oldSchemaName attribute but old_schema not found", newSchema.Name, newTable.Name)
		}
	} else if oldSchema == nil {
		GlobalDBSteward.Fatal("Sanity failure: %s.%s has oldTableName attribute but passed old_schema is not defined", newSchema.Name, newTable.Name)
	}

	oldTable = self.GetOldTable(newSchema, newTable)
	if oldTable == nil {
		GlobalDBSteward.Fatal("Sanity failure: %s.%s has oldTableName attribute, but table %s.%s not found", newSchema.Name, newTable.Name, oldSchema.Name, newTable.OldTableName)
	}
	return oldSchema, oldTable
}

func (self *DBX) IsRenamedTable(schema *model.Schema, table *model.Table) (bool, error) {
	if GlobalDBSteward.IgnoreOldNames {
		return false, nil
	}
	if table.OldTableName == "" {
		return false, nil
	}
	if schema.TryGetTableNamed(table.OldTableName) != nil {
		// TODO(feat) what if the table moves schemas?
		// TODO(feat) what if we move a table and replace it with a table of the same name?
		return true, errors.Errorf("oldTableName panic - new schema %s still contains table named %s", schema.Name, table.OldTableName)
	}

	oldSchema := self.GetOldTableSchema(schema, table)
	if oldSchema != nil {
		if oldSchema.TryGetTableNamed(table.OldTableName) == nil {
			return true, errors.Errorf("oldTableName panic - old schema %s does not contain table named %s", oldSchema.Name, table.OldTableName)
		}
	}

	// it is a new old named table rename if:
	// table.OldTableName exists in old schema
	// table.OldTableName does not exist in new schema
	if oldSchema.TryGetTableNamed(table.OldTableName) != nil && schema.TryGetTableNamed(table.OldTableName) == nil {
		GlobalDBSteward.Info("Table %s used to be called %s", table.Name, table.OldTableName)
		return true, nil
	}
	return false, nil
}

func (self *DBX) GetOldTableSchema(schema *model.Schema, table *model.Table) *model.Schema {
	if table.OldSchemaName == "" {
		return schema
	}
	if GlobalDBSteward.OldDatabase == nil {
		return nil
	}
	return GlobalDBSteward.OldDatabase.TryGetSchemaNamed(table.OldSchemaName)
}

func (self *DBX) GetOldTable(schema *model.Schema, table *model.Table) *model.Table {
	if table.OldTableName == "" {
		return nil
	}
	oldSchema := self.GetOldTableSchema(schema, table)
	return oldSchema.TryGetTableNamed(table.OldTableName)
}

func (self *DBX) TableDependencyOrder(doc *model.Definition) []*model.TableRef {
	// first, build forward and reverse adjacency lists
	// forwards: a mapping of local table => foreign tables that it references
	// reverse: a mapping of foreign table => local tables that reference it
	reverse := map[model.TableRef][]model.TableRef{}
	forward := util.NewOrderedMap[model.TableRef, *[]model.TableRef]()

	// init is used with GetOrInit to ensure we have a valid pointer-to-non-nil-slice
	init := func() *[]model.TableRef {
		return &[]model.TableRef{}
	}

	for _, schema := range doc.Schemas {
		for _, table := range schema.Tables {
			curr := model.TableRef{schema, table}
			if len(reverse[curr]) == 0 {
				reverse[curr] = []model.TableRef{}
			}

			// for each dependency of current table
			// add that dep as something this table depends on
			// add this table as something depending on that dep
			foreigns := forward.GetOrInit(curr, init)
			for _, dep := range self.getTableDependencies(doc, schema, table) {
				*foreigns = append(*foreigns, dep)
				reverse[dep] = append(reverse[dep], curr)
			}
		}
	}

	/*
		our goal is to produce a list of tables in an order such that creating the tables in order
		does not depend on any uncreated tables. we also need to fail out when a cycle is detected

		e.g. with a table graph like a -> b -> c
		                             d -<  >-> g   (d depends on both b and f, both b and f depend on g)
		                             e -> f
		then we might return: c, g, b, f, a, d, e
		                  or: g, c, b, a, f, e, d

		in this example, `forward` will contain what each table "points to"
		  a => b
		  b => c, g
		  c =>
		  d => b, f
		  e => f
		  f => g
		  g =>
		and `reverse` will contain what "points at" each table
		  a =>
		  b => a, d
		  c => b
		  d =>
		  e =>
		  f => d, e
		  g => b, f

		we know we can safely create any table that doesn't have any dependencies (which has no entries in `forward`)
		so, we add those to the list (c and g in this case), and remove it from the graph,
		using `reverse` to efficiently inform us which keys in `forward` to remove it from

		after one iteration we're left with `forward`:
		  a => b
		  b =>
		  d => b, f
		  e => f
		  f =>
		and `reverse`:
		  a =>
		  b => a, d
		  d =>
		  e =>
		  f => d, e

		now just rinse and repeat until there are no more tables in the adjacency lists.

		if at any point there are no entries in `forward` with len = 0, there is a cycle
	*/

	out := []*model.TableRef{}
	for forward.Len() > 0 {
		toRemove := []model.TableRef{}
		for _, entry := range forward.Entries() {
			local := entry.Key
			foreigns := entry.Value
			if len(*foreigns) == 0 {
				// GOTCHA: go reuses the same memory for loop iteration variables,
				// so we need to copy it before we make a pointer to it
				clone := local
				out = append(out, &clone)

				// mark it for removal. We need to do it in a separate pass so we don't mutate this loop slice
				toRemove = append(toRemove, local)
			}
		}
		for _, local := range toRemove {
			forward.Delete(local)
			for _, dependent := range reverse[local] {
				deps := forward.GetOrInit(dependent, init)
				*deps = util.Remove(*deps, local)
			}
			delete(reverse, local)
		}
		if len(toRemove) == 0 {
			// TODO(go,core) add diagnostics about what the cycle is
			GlobalDBSteward.Fatal("Dependency cycle detected!")
		}
		// fmt.Printf("current order: %v\n", out)
	}
	return out
}

func (self *DBX) getTableDependencies(doc *model.Definition, schema *model.Schema, table *model.Table) []model.TableRef {
	out := []model.TableRef{}
	// gather foreign keys on the columns
	for _, column := range table.Columns {
		if column.ForeignTable != "" {
			fref := GlobalDBX.ResolveSchemaTable(doc, schema, column.ForeignSchema, column.ForeignTable, "column foreignKey")
			out = append(out, fref)
		}
	}

	// gather explicit foreign keys
	for _, fk := range table.ForeignKeys {
		fref := GlobalDBX.ResolveSchemaTable(doc, schema, fk.ForeignSchema, fk.ForeignTable, "foreignKey element")
		out = append(out, fref)
	}

	// gather constraints
	for _, constraint := range table.Constraints {
		if constraint.ForeignTable != "" {
			fref := GlobalDBX.ResolveSchemaTable(doc, schema, constraint.ForeignSchema, constraint.ForeignTable, "FOREIGN KEY constraint")
			out = append(out, fref)
		}
	}

	// TODO(feat) examine <constraint type="FOREIGN KEY">
	// TODO(feat) any other dependencies from a table? sequences? inheritance?
	// TODO(feat) can we piggyback on Constraint.GetTableConstraints?
	return out
}

func (self *DBX) TryInheritanceGetColumn(doc *model.Definition, schema *model.Schema, table *model.Table, columnName string) *model.Column {
	// TODO(go,3) move to model
	column := table.TryGetColumnNamed(columnName)

	// just keep walking up the inheritance chain so long as there's a link
	for column == nil && table.InheritsTable != "" {
		ref := GlobalDBX.ResolveSchemaTable(doc, schema, table.InheritsSchema, table.InheritsTable, "inheritance")
		table = ref.Table
		column = table.TryGetColumnNamed(columnName)
	}

	return column
}

func (self *DBX) TryInheritanceGetColumns(doc *model.Definition, schema *model.Schema, table *model.Table, columnNames []string) ([]*model.Column, bool) {
	// TODO(go,nth) this could be more efficient (but more complicated) if we did all the columns at once, one table at a time
	columns := make([]*model.Column, len(columnNames))
	for i, colName := range columnNames {
		column := self.TryInheritanceGetColumn(doc, schema, table, colName)
		if column == nil {
			return nil, false
		}
		columns[i] = column
	}
	return columns, true
}
