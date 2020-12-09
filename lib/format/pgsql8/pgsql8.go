package pgsql8

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dbsteward/dbsteward/lib/format"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
)

var GlobalPgsql8 *Pgsql8 = NewPgsql8()

type Pgsql8 struct {
}

func NewPgsql8() *Pgsql8 {
	return &Pgsql8{}
}

func (self *Pgsql8) Build(outputPrefix string, dbDoc *model.Definition) {
	// TODO(go,4) can we just consider a build(def) to be diff(null, def)?
	// some shortcuts, since we're going to be typing a lot here
	dbsteward := lib.GlobalDBSteward
	xmlParser := lib.GlobalXmlParser
	sqlParser := lib.GlobalSqlParser
	dbx := lib.GlobalDBX

	buildFileName := outputPrefix + "_build.sql"
	dbsteward.Info("Building complete file %s", buildFileName)

	buildFile, err := os.OpenFile(buildFileName, os.O_RDWR, 0644)
	dbsteward.FatalIfError(err, "Failed to open file %s for output", buildFileName)

	buildFileOfs := lib.NewOutputFileSegmenter(buildFileName, 1, buildFile, buildFileName)
	if len(dbsteward.LimitToTables) == 0 {
		buildFileOfs.Write("-- full database definition file generated %s\n", time.Now().Format(time.RFC1123Z))
	}
	if !dbsteward.GenerateSlonik {
		buildFileOfs.Write("BEGIN;\n\n")
	}

	dbsteward.Info("Calculating table foreign dependency order...")
	tableDependency := xmlParser.TableDependencyOrder(dbDoc)

	// database-specific implementation code refers to dbsteward::$new_database when looking up roles/values/conflicts etc
	dbsteward.NewDatabase = dbDoc
	dbx.SetDefaultSchema(dbDoc, "public")

	// language definitions
	if dbsteward.CreateLanguages {
		for _, language := range dbDoc.Languages {
			buildFileOfs.WriteSql(GlobalLanguage.GetCreationSql(language)...)
		}
	}

	// by default, postgresql will validate the contents of LANGUAGE SQL functions during creation
	// because we are creating all functions before tables, this doesn't work when LANGUAGE SQL functions
	// refer to tables yet to be created.
	// scan language="sql" functions for <functionDefiniton>s that contain FROM (<TABLE>) statements
	setCheckFunctionBodies := true
	setCheckFunctionBodiesInfo := ""
outer:
	for _, schema := range dbDoc.Schemas {
		for _, function := range schema.Functions {
			if definition, ok := function.TryGetDefinition(); ok {
				if strings.EqualFold(definition.Language, "sql") && definition.SqlFormat == format.SqlFormatPgsql8 {
					referencedTableName := self.FunctionDefinitionReferencesTable(definition)
					if len(referencedTableName) > 0 {
						referencedSchemaName := sqlParser.GetSchemaName(referencedTableName, dbDoc)
						// TODO(go,pgsql8) handle error cases
						referencedSchema, _ := dbDoc.GetSchemaNamed(referencedSchemaName)
						referencedTable, err := referencedSchema.GetTableNamed(sqlParser.GetObjectName(referencedTableName, dbDoc))
						if err == nil {
							setCheckFunctionBodies = false
							setCheckFunctionBodiesInfo = fmt.Sprintf(
								"Detected LANGUAGE SQL function %s.%s referring to table %s.%s in the database definition",
								schema.Name, function.Name, referencedSchemaName, referencedTable.Name,
							)
							break outer
						}
					}
				}
			}
		}
	}
	if !setCheckFunctionBodies {
		buildFileOfs.Write("\n")
		buildFileOfs.WriteSql(&sql.SetCheckFunctionBodies{setCheckFunctionBodiesInfo})
		dbsteward.Info(setCheckFunctionBodiesInfo)
	}

	if dbsteward.OnlySchemaSql || !dbsteward.OnlyDataSql {
		dbsteward.Info("Defining structure")
		self.BuildSchema(dbDoc, buildFileOfs, tableDependency)
	}
	if !dbsteward.OnlySchemaSql || dbsteward.OnlyDataSql {
		dbsteward.Info("Defining data inserts")
		self.BuildData(dbDoc, buildFileOfs, tableDependency)
	}
	dbsteward.NewDatabase = nil

	if !dbsteward.GenerateSlonik {
		buildFileOfs.Write("COMMIT;\n\n")
	}

	if dbsteward.GenerateSlonik {
		// TODO(go,slony)
	}
}
func (self *Pgsql8) BuildUpgrade(
	oldOutputPrefix string, oldCompositeFile string, oldDoc *model.Definition, oldFiles []string,
	newOutputPrefix string, newCompositeFile string, newDoc *model.Definition, newFiles []string,
) {
	upgradePrefix := newOutputPrefix + "_upgrade"

	lib.GlobalDBSteward.Info("Calculating old table foreign key dependency order...")
	GlobalDiff.OldTableDependency = lib.GlobalXmlParser.TableDependencyOrder(oldDoc)

	lib.GlobalDBSteward.Info("Calculating new table foreign key dependency order...")
	GlobalDiff.NewTableDependency = lib.GlobalXmlParser.TableDependencyOrder(newDoc)

	GlobalDiff.DiffDoc(oldCompositeFile, newCompositeFile, oldDoc, newDoc, upgradePrefix)

	if lib.GlobalDBSteward.GenerateSlonik {
		// TODO(go,slony)
	}
}
func (self *Pgsql8) ExtractSchema(host string, port uint, name, user, pass string) *model.Definition {
	// TODO(go,pgsql) this one's gonna be a whole thing, probably need its own file
	return nil
}
func (self *Pgsql8) CompareDbData(doc *model.Definition, host string, port uint, name, user, pass string) *model.Definition {
	dbsteward := lib.GlobalDBSteward

	dbsteward.Notice("Connecting to pgsql8 host %s:%d database %s as user %s", host, port, name, user)
	GlobalDb.Connect(host, port, name, user, pass)

	dbsteward.Info("Comparing composited dbsteward definition data rows to postgresql database connection table contents")
	// compare the composited dbsteward document to the established database connection
	// effectively looking to see if rows are found that match primary keys, and if their contents are the same
	for _, schema := range doc.Schemas {
		for _, table := range schema.Tables {
			if table.Rows != nil {
				// TODO(go,nth) quote this
				tableName := fmt.Sprintf("%s.%s", schema.Name, table.Name)
				pkCols := table.PrimaryKey
				cols := table.Rows.Columns

				colTypes := map[string]string{}
				for _, column := range table.Columns {
					colType := column.Type

					// foreign keyed columns inherit their foreign reference type
					// TODO(go,3) this seems like something nice to have on the model
					if len(column.ForeignTable) > 0 && len(column.ForeignColumn) > 0 {
						if len(colType) > 0 {
							dbsteward.Fatal("type of %s was found for column %s but it is foreign keyed", colType, column)
						}
						foreign := lib.GlobalDBX.ForeignKey(doc, schema, table, column)
						colType = foreign.Column.Type
					}

					if len(colType) == 0 {
						dbsteward.Fatal("%s column %s type was not found", table.Name, column.Name)
					}

					colTypes[column.Name] = colType
				}

				for _, row := range table.Rows.Rows {
					// TODO(go,nth) can we fix this direct sql construction with a ToSql struct?
					pkExprs := []string{}
					for _, pkCol := range pkCols {
						// TODO(go,nth) can we put this column lookup in the model? `row.GetValueForColumn(pkCol)`
						pkIndex := lib.IndexOfStr(pkCol, cols)
						if pkIndex < 0 {
							dbsteward.Fatal("failed to find %s.%s primary key column %s in cols list %v",
								schema.Name, table.Name, pkCol, cols)
						}

						expr := fmt.Sprintf(
							"%s = %s",
							self.GetQuotedColumnName(pkCol),
							self.ValueEscape(colTypes[pkCol], row.Columns[pkIndex], doc),
						)
						pkExprs = append(pkExprs, expr)
					}
					pkExpr := strings.Join(pkExprs, " AND ")

					// TODO(go,nth) use parameterized queries
					sql := fmt.Sprintf(`SELECT * FROM %s WHERE %s`, tableName, pkExpr)
					res := GlobalDb.Query(sql)

					if row.Delete {
						if res.RowCount() > 0 {
							dbsteward.Notice("%s row marked for DELETE found WHERE %s", tableName, pkExpr)
						}
					} else if res.RowCount() == 0 {
						dbsteward.Notice("%s does not contain row WHERE %s", tableName, pkExpr)
					} else if res.RowCount() > 1 {
						dbsteward.Notice("%s contains more than one row WHERE %s", tableName, pkExpr)
						for res.Next() {
							dbRow := res.FetchRowStringMap()
							dbsteward.Notice("\t%v", dbRow)
						}
						// TODO(go,nth) error handling
					} else {
						res.Next()
						dbRow := res.FetchRowStringMap()
						for i, col := range cols {
							valuesMatch, xmlValue, dbValue := self.compareDbDataRow(colTypes[col], row.Columns[i], dbRow[col])
							if !valuesMatch {
								dbsteward.Warning("%s row column WHERE (%s) %s data does not match database row column: '%s' vs '%s'",
									tableName, pkExpr, col, xmlValue, dbValue)
							}
						}
					}
				}
			}
		}
	}
	return doc
}
func (self *Pgsql8) compareDbDataRow(colType, xmlValue, dbValue string) (bool, string, string) {
	colType = strings.ToLower(colType)
	xmlValue = self.pgdataHomogenize(colType, xmlValue)
	dbValue = self.pgdataHomogenize(colType, dbValue)
	if xmlValue == dbValue {
		return true, xmlValue, dbValue
	}

	// if they are not equal, and are alternately expressable, ask the database
	if strings.HasPrefix(colType, "time") || strings.HasPrefix(colType, "date") || strings.HasPrefix(colType, "interval") {
		if len(xmlValue) > 0 && len(dbValue) > 0 {
			sql := fmt.Sprintf(`SELECT $1::%s = $2::%[1]s`, colType)
			var eq bool
			err := GlobalDb.QueryVal(&eq, sql, xmlValue, dbValue)
			lib.GlobalDBSteward.FatalIfError(err, "Could not query database")
			return eq, xmlValue, dbValue
		}
	}

	return false, xmlValue, dbValue
}
func (self *Pgsql8) pgdataHomogenize(colType string, value string) string {
	switch {
	case strings.HasPrefix(colType, "bool"):
		switch strings.ToLower(value) {
		case "true", "t":
			return "true"
		default:
			return "false"
		}

	default:
		return value
	}
}

func (self *Pgsql8) SqlDiff(old, new []string, upgradePrefix string) {
	lib.GlobalDBSteward.Notice("Calculating sql differences:")
	lib.GlobalDBSteward.Notice("Old set: %v", old)
	lib.GlobalDBSteward.Notice("New set: %v", new)
	lib.GlobalDBSteward.Notice("Upgrade: %s", upgradePrefix)
	GlobalDiff.DiffSql(old, new, upgradePrefix)
}

func (self *Pgsql8) SlonyCompare(file string) {
	// TODO(go,slony)
}
func (self *Pgsql8) SlonyDiff(oldFile string, newFile string) {
	// TODO(go,slony)
}

func (self *Pgsql8) FunctionDefinitionReferencesTable(definition *model.FunctionDefinition) string {
	// TODO(go,pgsql8)
	return ""
}

func (self *Pgsql8) BuildSchema(doc *model.Definition, ofs lib.OutputFileSegmenter, tableDep []*lib.TableDepEntry) {
	// schema creation
	for _, schema := range doc.Schemas {
		ofs.WriteSql(GlobalSchema.GetCreationSql(schema)...)

		// schema grants
		for _, grant := range schema.Grants {
			// TODO(feat) revokes too?
			ofs.WriteSql(GlobalPermission.GetGrantSql(doc, schema, schema, grant)...)
		}
	}

	// types: enumerated list, etc
	for _, schema := range doc.Schemas {
		for _, datatype := range schema.Types {
			ofs.WriteSql(GlobalDataType.GetCreationSql(schema, datatype)...)
		}
	}

	// table structure creation
	for _, schema := range doc.Schemas {
		// create defined tables
		GlobalTable.IncludeColumnDefaultNextvalInCreateSql = false
		for _, table := range schema.Tables {
			// table definition
			ofs.WriteSql(GlobalTable.GetCreationSql(schema, table)...)

			// table indexes
			GlobalDiffIndexes.DiffIndexesTable(ofs, nil, nil, schema, table)

			// table grants
			for _, grant := range table.Grants {
				// TODO(feat) revokes too?
				ofs.WriteSql(GlobalPermission.GetGrantSql(doc, schema, table, grant)...)
			}
		}
		GlobalTable.IncludeColumnDefaultNextvalInCreateSql = true

		// sequences contained in the schema
		for _, sequence := range schema.Sequences {
			ofs.WriteSql(GlobalSequence.GetCreationSql(schema, sequence)...)

			// sequence permission grants
			for _, grant := range sequence.Grants {
				// TODO(feat) revokes too?
				ofs.WriteSql(GlobalPermission.GetGrantSql(doc, schema, sequence, grant)...)
			}
		}

		// add table nextvals that were omitted
		for _, table := range schema.Tables {
			if table.HasDefaultNextVal() {
				ofs.WriteSql(GlobalTable.GetDefaultNextvalSql(schema, table)...)
			}
		}
	}

	// function definitions
	for _, schema := range doc.Schemas {
		for _, function := range schema.Functions {
			if function.HasDefinition() {
				ofs.WriteSql(GlobalFunction.GetCreationSql(schema, function)...)
				// when pg:build_schema() is doing its thing for straight builds, include function permissions
				// they are not included in pg_function::get_creation_sql()

				// TODO(feat) functions generate sql for both grant and revoke, but other objects only do grant? can we unify this?
				// TODO(go,pgsql) verify that order of this doesn't matter. this code does grants then revokes, orig does them in xpath order
				for _, grant := range function.Grants {
					ofs.WriteSql(GlobalPermission.GetGrantSql(doc, schema, function, grant)...)
				}
				for _, revoke := range function.Revokes {
					ofs.WriteSql(GlobalPermission.GetRevokeSql(doc, schema, function, revoke)...)
				}
			}
		}
	}

	// maybe move this but here we're defining column defaults fo realz
	for _, schema := range doc.Schemas {
		for _, table := range schema.Tables {
			// TODO(go,nth) method name consistency - should be GetColumnDefaultsSql?
			ofs.WriteSql(GlobalTable.DefineTableColumnDefaults(schema, table)...)
		}
	}

	// define table primary keys before foreign keys so unique requirements are always met for FOREIGN KEY constraints
	for _, schema := range doc.Schemas {
		for _, table := range schema.Tables {
			GlobalDiffTables.DiffConstraintsTable(ofs, nil, nil, schema, table, "primaryKey", false)
		}
	}

	// foreign key references
	// use the dependency order to specify foreign keys in an order that will satisfy nested foreign keys and etc
	// TODO(feat) shouldn't this consider GlobalDBSteward.LimitToTables like BuildData does?
	for _, entry := range tableDep {
		if entry.IgnoreEntry {
			continue
		}

		GlobalDiffTables.DiffConstraintsTable(ofs, nil, nil, entry.Schema, entry.Table, "constraint", false)
	}

	// trigger definitions
	for _, schema := range doc.Schemas {
		for _, trigger := range schema.Triggers {
			if trigger.SqlFormat == format.SqlFormatPgsql8 {
				ofs.WriteSql(GlobalTrigger.GetCreationSql(schema, trigger)...)
			}
		}
	}

	GlobalDiffViews.CreateViewsOrdered(ofs, nil, doc)

	// view permission grants
	for _, schema := range doc.Schemas {
		for _, view := range schema.Views {
			for _, grant := range view.Grants {
				// TODO(feat) revokes too?
				ofs.WriteSql(GlobalPermission.GetGrantSql(doc, schema, view, grant)...)
			}
		}
	}

	GlobalDiff.UpdateDatabaseConfigParameters(ofs, nil, doc)
}

func (self *Pgsql8) BuildData(doc *model.Definition, ofs lib.OutputFileSegmenter, tableDep []*lib.TableDepEntry) {
	limitToTables := lib.GlobalDBSteward.LimitToTables

	// use the dependency order to then write out the actual data inserts into the data sql file
	for _, entry := range tableDep {
		if entry.IgnoreEntry {
			continue
		}
		schema := entry.Schema
		table := entry.Table

		// skip any tables that are not in the limit list, if there are any tables to limit
		if len(limitToTables) > 0 {
			if includeTables, ok := limitToTables[schema.Name]; ok {
				if !lib.InArrayStr(table.Name, includeTables) {
					continue
				}
			} else {
				// if this entry's schema didn't appear in the include list, we can't possibly include any tables from it
				continue
			}
		}

		ofs.WriteSql(GlobalDiffTables.GetDataSql(nil, nil, schema, table, false)...)

		// set serial primary keys to the max value after inserts have been performed
		// only if the PRIMARY KEY is not a multi column
		if table.Rows != nil && len(table.PrimaryKey) == 1 {
			dataCols := table.Rows.Columns
			pkCol := table.PrimaryKey[0]
			if lib.InArrayStr(pkCol, dataCols) {
				// TODO(go,3) seems like this could be refactored better by putting much of the lookup
				// into the model structs
				cols := lib.GlobalXmlParser.InheritanceGetColumn(table, pkCol)
				if len(cols) != 1 {
					lib.GlobalDBSteward.Fatal("Failed to find primary key column '%s' for %s.%s",
						pkCol, schema.Name, table.Name)
				}
				pk := cols[0]
				if GlobalDataType.IsLinkedTableType(pk.Type) {
					if len(pk.SerialStart) > 0 {
						ofs.WriteSql(&sql.SetValSerialSequenceMax{
							Schema: schema.Name,
							Table:  table.Name,
							Column: pk.Name,
						})
					}
				}
			}
		}

		// check if primary key columns are columns of this table
		// TODO(feat) does this check belong here? should there be some kind of post-parse validation?
		for _, columnName := range table.PrimaryKey {
			cols := lib.GlobalXmlParser.InheritanceGetColumn(table, columnName)
			if len(cols) != 1 {
				lib.GlobalDBSteward.Fatal("Declared primary key column (%s) does not exist as column in table %s.%s",
					columnName, schema.Name, table.Name)
			}
		}
	}

	// include all of the unstaged sql elements
	lib.GlobalDBX.BuildStagedSql(doc, ofs, -1)
}

func (self *Pgsql8) GetQuotedColumnName(name string) string {
	// TODO(go,pgsql8) quoting
	return name
}

func (self *Pgsql8) ValueEscape(datatype string, value string, doc *model.Definition) string {
	// TODO(go,pgsql8) see pgsql8::value_escape()
	// TODO(go,3) it'd be amazing to have a dedicated Value type that encapsulates this logic and is type-aware, instead of the mishmash of string parsing and type matching we do
	return value
}
