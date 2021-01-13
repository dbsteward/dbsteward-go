package lib

import (
	"strings"

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
	fSchema, fTable := self.ResolveSchemaTable(doc, localKey.Schema, foreignKey.Schema, foreignKey.Table, "foreign key")

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

func (self *DBX) ResolveSchemaTable(doc *model.Definition, localSchema *model.Schema, schemaName, tableName string, refType string) (*model.Schema, *model.Table) {
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

	return fSchema, fTable
}

func (self *DBX) EnumRegex(doc *model.Definition) string {
	// TODO(go,core) dbx::enum_regex()
	return ""
}

func (self *DBX) RenamedTableCheckPointer(oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) (*model.Schema, *model.Table) {
	// TODO(go,core) dbx::renamed_table_check_pointer()
	return nil, nil
}

func (self *DBX) TableDependencyOrder(doc *model.Definition) []*model.TableRef {
	// first, build forward and reverse adjacency lists
	// forwards: a mapping of local table => foreign tables that it references
	// reverse: a mapping of foreign table => local tables that reference it
	forward := map[model.TableRef][]model.TableRef{}
	reverse := map[model.TableRef][]model.TableRef{}
	for _, schema := range doc.Schemas {
		for _, table := range schema.Tables {
			curr := model.TableRef{schema, table}
			// initialize them so we know the node is there, even if it has no dependencies
			if len(forward[curr]) == 0 {
				forward[curr] = []model.TableRef{}
			}
			if len(reverse[curr]) == 0 {
				reverse[curr] = []model.TableRef{}
			}

			for _, dep := range self.getTableDependencies(doc, schema, table) {
				forward[curr] = append(forward[curr], dep)
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

	// a quick helper to cut down on complexity below, see https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	// HACK: this is, IMHO, a really bullshit and footgunny method to do this efficiently
	remove := func(target model.TableRef, slice []model.TableRef) []model.TableRef {
		b := slice[:0]
		for _, x := range slice {
			if x != target {
				b = append(b, x)
			}
		}
		// garbage collect
		for i := len(b); i < len(slice); i++ {
			slice[i] = model.TableRef{}
		}
		return b
	}

	out := []*model.TableRef{}
	i := 0
	for len(forward) > 0 {
		// fmt.Printf("%d ----\n", i)
		// fmt.Printf("forward:\n", name)
		// for key, vals := range forward {
		// 	fmt.Printf("  %s => %v\n", key, vals)
		// }
		// fmt.Printf("reverse:\n", name)
		// for key, vals := range reverse {
		// 	fmt.Printf("  %s => %v\n", key, vals)
		// }
		i += 1
		atLeastOne := false
		for local, foreigns := range forward {
			if len(foreigns) == 0 {
				// fmt.Printf("%s has no foreigns\n", local)
				// GOTCHA: go reuses the same memory for loop iteration variables,
				// so we need to copy it before we make a pointer to it
				clone := local
				out = append(out, &clone)
				atLeastOne = true

				// remove it from the graph now
				delete(forward, local)
				for _, dependent := range reverse[local] {
					forward[dependent] = remove(local, forward[dependent])
				}
				delete(reverse, local)
			}
		}
		if !atLeastOne {
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
			fSchema, fTable := GlobalDBX.ResolveSchemaTable(doc, schema, column.ForeignSchema, column.ForeignTable, "column foreignKey")
			out = append(out, model.TableRef{fSchema, fTable})
		}
	}

	// gather explicit foreign keys
	for _, fk := range table.ForeignKeys {
		fSchema, fTable := GlobalDBX.ResolveSchemaTable(doc, schema, fk.ForeignSchema, fk.ForeignTable, "foreignKey element")
		out = append(out, model.TableRef{fSchema, fTable})
	}

	// gather constraints
	for _, constraint := range table.Constraints {
		if constraint.ForeignTable != "" {
			fSchema, fTable := GlobalDBX.ResolveSchemaTable(doc, schema, constraint.ForeignSchema, constraint.ForeignTable, "FOREIGN KEY constraint")
			out = append(out, model.TableRef{fSchema, fTable})
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
		schema, table = GlobalDBX.ResolveSchemaTable(doc, schema, table.InheritsSchema, table.InheritsTable, "inheritance")
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
