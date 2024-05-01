package ir

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/pkg/errors"

	"github.com/dbsteward/dbsteward/lib/util"
)

// TODO(go,3) move most public fields to private, use accessors to better enable encapsulation, validation; "make invalid states unrepresentable"

type Definition struct {
	IncludeFiles   []*IncludeFile
	InlineAssembly []*InlineAssembly
	Database       *Database
	Schemas        []*Schema
	Languages      []*Language
	Sql            []*Sql
}

type IncludeFile struct {
	Name string
}

type InlineAssembly struct {
	Name string
}

type Sql struct {
	Author  string
	Ticket  string
	Version string
	Comment string
	Stage   SqlStage
	Text    string
}

func (def *Definition) GetSchemaNamed(name string) (*Schema, error) {
	matching := []*Schema{}
	for _, schema := range def.Schemas {
		// TODO(feat) case insensitivity?
		if schema.Name == name {
			matching = append(matching, schema)
		}
	}
	if len(matching) == 0 {
		return nil, errors.Errorf("no schema named %s found", name)
	}
	if len(matching) > 1 {
		return nil, errors.Errorf("more than one schema named %s found", name)
	}
	return matching[0], nil
}

func (def *Definition) TryGetSchemaNamed(name string) *Schema {
	if def == nil {
		return nil
	}
	for _, schema := range def.Schemas {
		// TODO(feat) case insensitivity?
		if schema.Name == name {
			return schema
		}
	}
	return nil
}

func (def *Definition) GetOrCreateSchemaNamed(name string) *Schema {
	schema := def.TryGetSchemaNamed(name)
	if schema == nil {
		schema = &Schema{Name: name}
		def.AddSchema(schema)
	}
	return schema
}

func (def *Definition) AddSchema(schema *Schema) {
	// TODO(feat) sanity check duplicate name
	def.Schemas = append(def.Schemas, schema)
}

func (def *Definition) TryGetLanguageNamed(name string) *Language {
	if def == nil {
		return nil
	}
	for _, lang := range def.Languages {
		// TODO(feat) case insensitivity
		if lang.Name == name {
			return lang
		}
	}
	return nil
}
func (def *Definition) AddLanguage(lang *Language) {
	// TODO(feat) sanity check
	def.Languages = append(def.Languages, lang)
}

func (def *Definition) IsRoleDefined(role string) bool {
	if util.IStrsContains(MACRO_ROLES, role) {
		return true
	}
	if def.Database == nil {
		return false
	}
	return def.Database.IsRoleDefined(role)
}

func (def *Definition) AddCustomRole(role string) {
	if def.Database == nil {
		// TODO(go,nth) incomplete construction
		def.Database = &Database{}
	}
	def.Database.AddCustomRole(role)
}

func (def *Definition) TryGetSqlMatching(target *Sql) *Sql {
	if def == nil {
		return nil
	}
	for _, sql := range def.Sql {
		if sql.IdentityMatches(target) {
			return sql
		}
	}
	return nil
}

func (def *Definition) AddSql(sql *Sql) {
	// TODO(feat) sanity check
	def.Sql = append(def.Sql, sql)
}

// Merge is the new implementation of xml_parser::xml_composite_children
// it merges items from the overlay into this definition
func (def *Definition) Merge(overlay *Definition) {
	if overlay == nil {
		return
	}
	def.IncludeFiles = append(def.IncludeFiles, overlay.IncludeFiles...)
	def.InlineAssembly = append(def.InlineAssembly, overlay.InlineAssembly...)

	if def.Database == nil {
		def.Database = &Database{}
	}
	def.Database.Merge(overlay.Database)

	for _, overlaySchema := range overlay.Schemas {
		if baseSchema := def.TryGetSchemaNamed(overlaySchema.Name); baseSchema != nil {
			baseSchema.Merge(overlaySchema)
		} else {
			// TODO(go,core) we should probably clone this. Should we just make AddSchema take a value not pointer?
			def.AddSchema(overlaySchema)
		}
	}

	for _, overlayLang := range overlay.Languages {
		if baseLang := def.TryGetLanguageNamed(overlayLang.Name); baseLang != nil {
			baseLang.Merge(overlayLang)
		} else {
			def.AddLanguage(overlayLang)
		}
	}

	for _, overlaySql := range overlay.Sql {
		if baseSql := def.TryGetSqlMatching(overlaySql); baseSql != nil {
			baseSql.Merge(overlaySql)
		} else {
			def.AddSql(overlaySql)
		}
	}
}

// Validate is the new implementation of the various validation operations
// that occur throughout the codebase. It detects issues with the database
// schema that a user will need to address. (This is NOT to detect an invalidly
// constructed Definition object / programming errors in dbsteward itdef)
// This is initially intended to replace only the validations that occur in
// xml_parser::xml_composite_children()
// TODO(go,3) can we replace this with a more generic visitor pattern?
// TODO(go,3) should there be warnings?
func (def *Definition) Validate() []error {
	out := []error{}

	// no two objects should have the same identity (also, validate sub-objects)
	for i, schema := range def.Schemas {
		out = append(out, schema.Validate(def)...)
		for _, other := range def.Schemas[i+1:] {
			if schema.IdentityMatches(other) {
				out = append(out, fmt.Errorf("found two schemas with name %q", schema.Name))
			}
		}
	}

	for i, sql := range def.Sql {
		out = append(out, sql.Validate(def)...)
		for _, other := range def.Sql[i+1:] {
			if sql.IdentityMatches(other) {
				// TODO(go,nth) better identifier for sql?
				out = append(out, fmt.Errorf("found two sql elements with text %q", sql.Text))
			}
		}
	}

	return out
}

// TryGetTableFormerlyKnownAs attempts to find the new table that claims it
// is renamed from the old table.
func (newDoc *Definition) TryGetTableFormerlyKnownAs(oldSchema *Schema, oldTable *Table) *TableRef {
	// TODO(go,3) move to model, and/or compositing pass
	for _, newSchema := range newDoc.Schemas {
		for _, newTable := range newSchema.Tables {
			if newTable.OldTableName != "" || newTable.OldSchemaName != "" {
				oldTableName := util.CoalesceStr(newTable.OldTableName, newTable.Name)
				oldSchemaName := util.CoalesceStr(newTable.OldSchemaName, newSchema.Name)
				if strings.EqualFold(oldSchema.Name, oldSchemaName) && strings.EqualFold(oldTable.Name, oldTableName) {
					return &TableRef{Schema: newSchema, Table: newTable}
				}
			}
		}
	}
	return nil
}

func (doc *Definition) ResolveSchemaTable(localSchema *Schema, schemaName, tableName string, refType string) (TableRef, error) {
	fSchema := localSchema
	if schemaName != "" {
		fSchema = doc.TryGetSchemaNamed(schemaName)
		if fSchema == nil {
			return TableRef{}, fmt.Errorf("%s reference to unknown schema %s", refType, schemaName)
		}
	}

	fTable := fSchema.TryGetTableNamed(tableName)
	if fTable == nil {
		return TableRef{}, fmt.Errorf("%s reference to unknown table %s.%s", refType, fSchema.Name, tableName)
	}

	return TableRef{Schema: fSchema, Table: fTable}, nil
}

func (doc *Definition) TryInheritanceGetColumn(schema *Schema, table *Table, columnName string) (*Column, error) {
	// TODO(go,3) move to model
	column := table.TryGetColumnNamed(columnName)

	// just keep walking up the inheritance chain so long as there's a link
	for column == nil && table.InheritsTable != "" {
		ref, err := doc.ResolveSchemaTable(schema, table.InheritsSchema, table.InheritsTable, "inheritance")
		if err != nil {
			return nil, err
		}
		table = ref.Table
		column = table.TryGetColumnNamed(columnName)
	}

	return column, nil
}

func (oldDatabase *Definition) GetOldTableSchema(schema *Schema, table *Table) *Schema {
	if table.OldSchemaName == "" {
		return schema
	}
	if oldDatabase == nil {
		return nil
	}
	return oldDatabase.TryGetSchemaNamed(table.OldSchemaName)
}

func (oldDatabase *Definition) GetOldTable(schema *Schema, table *Table) *Table {
	if table.OldTableName == "" {
		return nil
	}
	oldSchema := oldDatabase.GetOldTableSchema(schema, table)
	return oldSchema.TryGetTableNamed(table.OldTableName)
}

func (doc *Definition) TryInheritanceGetColumns(schema *Schema, table *Table, columnNames []string) ([]*Column, error) {
	// TODO(go,nth) this could be more efficient (but more complicated) if we did all the columns at once, one table at a time
	columns := make([]*Column, len(columnNames))
	for i, colName := range columnNames {
		column, err := doc.TryInheritanceGetColumn(schema, table, colName)
		if err != nil {
			return nil, fmt.Errorf("resolving column %s: %w", colName, err)
		}
		if column == nil {
			return nil, fmt.Errorf("column %s not found", colName)
		}
		columns[i] = column
	}
	return columns, nil
}

func (doc *Definition) TableDependencyOrder() ([]*TableRef, error) {
	// first, build forward and reverse adjacency lists
	// forwards: a mapping of local table => foreign tables that it references
	// reverse: a mapping of foreign table => local tables that reference it
	reverse := map[TableRef][]TableRef{}
	forward := util.NewOrderedMap[TableRef, *[]TableRef]()

	// init is used with GetOrInit to ensure we have a valid pointer-to-non-nil-slice
	init := func() *[]TableRef {
		return &[]TableRef{}
	}

	for _, schema := range doc.Schemas {
		for _, table := range schema.Tables {
			curr := TableRef{Schema: schema, Table: table}
			if len(reverse[curr]) == 0 {
				reverse[curr] = []TableRef{}
			}

			// for each dependency of current table
			// add that dep as something this table depends on
			// add this table as something depending on that dep
			foreigns := forward.GetOrInit(curr, init)
			deps, err := doc.getTableDependencies(schema, table)
			if err != nil {
				return nil, err
			}
			for _, dep := range deps {
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

	out := []*TableRef{}
	for forward.Len() > 0 {
		toRemove := []TableRef{}
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
			return nil, errors.New("Dependency cycle detected!")
		}
		// fmt.Printf("current order: %v\n", out)
	}
	return out, nil
}

func (doc *Definition) getTableDependencies(schema *Schema, table *Table) ([]TableRef, error) {
	out := []TableRef{}
	// gather foreign keys on the columns
	for _, column := range table.Columns {
		if column.ForeignTable != "" {
			fref, err := doc.ResolveSchemaTable(schema, column.ForeignSchema, column.ForeignTable, "column foreignKey")
			if err != nil {
				return nil, fmt.Errorf("gathering foreign keys: %w", err)
			}
			out = append(out, fref)
		}
	}

	// gather explicit foreign keys
	for _, fk := range table.ForeignKeys {
		fref, err := doc.ResolveSchemaTable(schema, fk.ForeignSchema, fk.ForeignTable, "foreignKey element")
		if err != nil {
			return nil, fmt.Errorf("gathering explicit foreign keys: %w", err)
		}
		out = append(out, fref)
	}

	// gather constraints
	for _, constraint := range table.Constraints {
		if constraint.ForeignTable != "" {
			fref, err := doc.ResolveSchemaTable(schema, constraint.ForeignSchema, constraint.ForeignTable, "FOREIGN KEY constraint")
			if err != nil {
				return nil, fmt.Errorf("gathering constraints: %w", err)
			}
			out = append(out, fref)
		}
	}

	// TODO(feat) examine <constraint type="FOREIGN KEY">
	// TODO(feat) any other dependencies from a table? sequences? inheritance?
	// TODO(feat) can we piggyback on Constraint.GetTableConstraints?
	return out, nil
}

func (oldDatabase *Definition) IsRenamedTable(l *slog.Logger, schema *Schema, table *Table) (bool, error) {
	if table.OldTableName == "" {
		return false, nil
	}
	if schema.TryGetTableNamed(table.OldTableName) != nil {
		// TODO(feat) what if the table moves schemas?
		// TODO(feat) what if we move a table and replace it with a table of the same name?
		return true, errors.Errorf("oldTableName panic - new schema %s still contains table named %s", schema.Name, table.OldTableName)
	}

	oldSchema := oldDatabase.GetOldTableSchema(schema, table)
	if oldSchema != nil {
		if oldSchema.TryGetTableNamed(table.OldTableName) == nil {
			return true, errors.Errorf("oldTableName panic - old schema %s does not contain table named %s", oldSchema.Name, table.OldTableName)
		}
	}

	// it is a new old named table rename if:
	// table.OldTableName exists in old schema
	// table.OldTableName does not exist in new schema
	if oldSchema.TryGetTableNamed(table.OldTableName) != nil && schema.TryGetTableNamed(table.OldTableName) == nil {
		l.Info(fmt.Sprintf("Table %s used to be called %s", table.Name, table.OldTableName))
		return true, nil
	}
	return false, nil
}

// attempts to find, and sanity checks, the table pointed to by oldSchema/TableName attributes
// this is the "backwards looking" version of TryGetTableFormerlyKnownAs
// TODO(go,nth) rename this, clean it up
func (oldDatabase *Definition) NewTableName(oldSchema *Schema, oldTable *Table, newSchema *Schema, newTable *Table) (*Schema, *Table, error) {
	if newSchema == nil || newTable == nil {
		return oldSchema, oldTable, nil
	}

	isRenamed, err := oldDatabase.IsRenamedTable(slog.Default(), newSchema, newTable)
	if err != nil {
		return nil, nil, fmt.Errorf("while checking table rename status: %w", err)
	}
	if !isRenamed {
		return oldSchema, oldTable, nil
	}

	if newTable.OldSchemaName != "" {
		oldSchema = oldDatabase.GetOldTableSchema(newSchema, newTable)
		if oldSchema == nil {
			return nil, nil, fmt.Errorf("sanity failure: %s.%s has oldSchemaName attribute but old_schema not found", newSchema.Name, newTable.Name)
		}
	} else if oldSchema == nil {
		return nil, nil, fmt.Errorf("sanity failure: %s.%s has oldTableName attribute but passed old_schema is not defined", newSchema.Name, newTable.Name)
	}

	oldTable = oldDatabase.GetOldTable(newSchema, newTable)
	if oldTable == nil {
		return nil, nil, fmt.Errorf("sanity failure: %s.%s has oldTableName attribute, but table %s.%s not found", newSchema.Name, newTable.Name, oldSchema.Name, newTable.OldTableName)
	}
	return oldSchema, oldTable, nil
}

func (doc *Definition) ResolveForeignKey(localKey Key, foreignKey KeyNames) (Key, error) {
	fref, err := doc.ResolveSchemaTable(localKey.Schema, foreignKey.Schema, foreignKey.Table, "foreign key")
	if err != nil {
		return Key{}, fmt.Errorf("gathering foreign keys: %w", err)
	}

	// if we didn't ask for specific foreign columns, but we have local columns, use those
	if len(foreignKey.Columns) == 0 {
		util.Assert(len(localKey.Columns) > 0, "Called ResolveForeignKey with no columns to resolve in either localKey or foreignKey")
		foreignKey.Columns = make([]string, len(localKey.Columns))
	}

	if len(localKey.Columns) != len(foreignKey.Columns) {
		return Key{}, fmt.Errorf("local %s has column count mismatch with foreign %s", localKey.String(), foreignKey.String())
	}

	out := Key{
		Schema:  fref.Schema,
		Table:   fref.Table,
		Columns: make([]*Column, len(foreignKey.Columns)),
		KeyName: foreignKey.KeyName,
	}

	for i, col := range foreignKey.Columns {
		// if the foreign column wasn't specified, use the local column name
		if col == "" {
			util.Assert(localKey.Columns[i] != nil && localKey.Columns[i].Name != "",
				"Called ResolveForeignKey with an empty foreign column but local column name is missing or nil")
			col = localKey.Columns[i].Name
		}

		fCol, err := doc.TryInheritanceGetColumn(fref.Schema, fref.Table, col)
		if err != nil {
			return Key{}, fmt.Errorf("running TryInheritanceGetColumn: %w", err)
		}
		if fCol == nil {
			return Key{}, fmt.Errorf("failed to find foreign column %s in %s referenced by %s", col, foreignKey.String(), localKey.String())
		}
		out.Columns[i] = fCol
	}

	return out, nil
}

func (doc *Definition) GetTerminalForeignColumn(l *slog.Logger, schema *Schema, table *Table, column *Column) (*Column, error) {
	fkey, err := doc.ResolveForeignKeyColumn(schema, table, column)
	if err != nil {
		return nil, err
	}
	fcol := fkey.Columns[0]

	if fcol.Type == "" && fcol.ForeignTable != "" {
		l.Debug(fmt.Sprintf("Seeking nested foreign key for %s", fkey.String()))
		return doc.GetTerminalForeignColumn(l, fkey.Schema, fkey.Table, fcol)
	}
	return fcol, err
}

func (doc *Definition) ResolveForeignKeyColumn(schema *Schema, table *Table, column *Column) (Key, error) {
	// this used to be called format_constraint::foreign_key_lookup() in v1
	// most of the functionality got split to the more general ResolveForeignKey
	foreign := column.TryGetReferencedKey()
	util.Assert(foreign != nil, "ResolveForeignKeyColumn called with column that does not reference a foreign column")

	local := Key{
		Schema:  schema,
		Table:   table,
		Columns: []*Column{column},
	}
	return doc.ResolveForeignKey(local, *foreign)
}

func (s *Sql) IdentityMatches(other *Sql) bool {
	if other == nil {
		return false
	}
	// TODO(feat) make this more sophisticated
	return s.Text == other.Text
}

func (s *Sql) Merge(overlay *Sql) {
	if overlay == nil {
		return
	}
	s.Author = overlay.Author
	s.Ticket = overlay.Ticket
	s.Version = overlay.Version
	s.Comment = overlay.Comment
	s.Stage = overlay.Stage
}

func (s *Sql) Validate(*Definition) []error {
	return nil
}
