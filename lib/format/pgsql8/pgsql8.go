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
	oldOutputPrefix string, oldCompositeFile string, oldDbDoc *model.Definition, oldFiles []string,
	newOutputPrefix string, newCompositeFile string, newDbDoc *model.Definition, newFiles []string,
) {
	// TODO(go,pgsql)
}
func (self *Pgsql8) ExtractSchema(host string, port uint, name, user, pass string) *model.Definition {
	// TODO(go,pgsql)
	return nil
}
func (self *Pgsql8) CompareDbData(dbDoc *model.Definition, host string, port uint, name, user, pass string) *model.Definition {
	// TODO(go,pgsql)
	return nil
}
func (self *Pgsql8) SqlDiff(old, new, outputFile string) {
	// TODO(go,pgsql)
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
				ofs.WriteSql(GlobalPermission.GetGrantSql(doc, schema, table, grant)...)
			}
		}
		GlobalTable.IncludeColumnDefaultNextvalInCreateSql = true

		// sequences contained in the schema
		for _, sequence := range schema.Sequences {
			ofs.WriteSql(GlobalSequence.GetCreationSql(schema, sequence)...)

			// sequence permission grants
			for _, grant := range sequence.Grants {
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
				ofs.WriteSql(GlobalPermission.GetGrantSql(doc, schema, view, grant)...)
			}
		}
	}

	GlobalDiff.UpdateDatabaseConfigParameters(ofs, nil, doc)
}

func (self *Pgsql8) BuildData(doc *model.Definition, ofs lib.OutputFileSegmenter, tableDep interface{}) {
	// TODO(go,pgsql)
}
