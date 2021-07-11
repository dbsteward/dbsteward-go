package mysql5

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/format/sql99"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

type Operations struct {
	*sql99.Operations

	UseSchemaNamePrefix bool
}

func NewOperations() *Operations {
	ops := &Operations{
		Operations: sql99.NewOperations(),
	}
	ops.Operations.Operations = ops
	return ops
}

func (self *Operations) Build(outputPrefix string, dbDoc *model.Definition) {
	dbsteward := lib.GlobalDBSteward
	dbx := lib.GlobalDBX

	buildFileName := outputPrefix + "_build.sql"
	dbsteward.Info("Building complete file %s", buildFileName)

	buildFile, err := os.Create(buildFileName)
	dbsteward.FatalIfError(err, "Failed to open file %s for output", buildFileName)

	buildFileOfs := output.NewOutputFileSegmenterToFile(dbsteward, self.GetQuoter(), buildFileName, 1, buildFile, buildFileName, dbsteward.OutputFileStatementLimit)
	if len(dbsteward.LimitToTables) == 0 {
		buildFileOfs.Write("-- full database definition file generated %s\n", time.Now().Format(time.RFC1123Z))
	}

	// TODO(feat):
	// $build_file_ofs->write("START TRANSACTION;\n\n");

	dbsteward.Info("Calculating table foreign dependency order...")
	tableDependency := dbx.TableDependencyOrder(dbDoc)

	// database-specific implementation code refers to dbsteward::$new_database when looking up roles/values/conflicts etc
	dbsteward.NewDatabase = dbDoc

	// language definitions
	if dbsteward.CreateLanguages {
		for _, language := range dbDoc.Languages {
			dbsteward.Warning("Ignoring langauge %s because MySQL does not support languages other than 'sql'", language.Name)
		}
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

	// TODO(feat):
	// $build_file_ofs->write("COMMIT TRANSACTION;\n\n");
}

func (self *Operations) BuildSchema(doc *model.Definition, ofs output.OutputFileSegmenter, tableDep []*model.TableRef) {
	// TODO(go,3) roll this into diffing nil->doc
	dbsteward := lib.GlobalDBSteward

	if self.UseSchemaNamePrefix {
		dbsteward.Info("MySQL schema name prefixing mode turned on")
	} else if len(doc.Schemas) > 1 {
		dbsteward.Fatal("Found %d schemas but only 1 is allowed without enabling schema name prefix mode with --useschemaprefix", len(doc.Schemas))
	}

	for _, schema := range doc.Schemas {
		// database grants
		for _, grant := range schema.Grants {
			ofs.WriteSql(GlobalSchema.GetGrantSql(doc, schema, grant)...)
		}

		// enums
		for _, datatype := range schema.Types {
			sql, err := GlobalDataType.GetCreationSql(schema, datatype)
			dbsteward.FatalIfError(err, "Could not get data type creation sql for build")
			ofs.WriteSql(sql...)
		}

		// function definitions
		for _, function := range schema.Functions {
			if function.HasDefinition(model.SqlFormatMysql5) {
				ofs.WriteSql(GlobalFunction.GetCreationSql(schema, function)...)
				for _, grant := range function.Grants {
					ofs.WriteSql(GlobalFunction.GetGrantSql(doc, schema, function, grant)...)
				}
			}
		}

		sequences := []*model.Sequence{}
		triggers := []*model.Trigger{}

		// create defined tables
		for _, table := range schema.Tables {
			// TODO(go,mysql) should this be more than an append?
			sequences = append(sequences, GlobalTable.GetSequencesNeeded(schema, table)...)
			triggers = append(triggers, GlobalTable.GetTriggersNeeded(schema, table)...)

			ofs.WriteSql(GlobalTable.GetCreationSql(schema, table)...)

			// TODO(go,mysql) what is this? see mysql.php:152
			// table indexes
			// GlobalDiffIndexes.DiffIndexesTable(ofs, nil, nil, schema, table)

			// table grants
			for _, grant := range table.Grants {
				ofs.WriteSql(GlobalTable.GetGrantSql(doc, schema, table, grant)...)
			}
		}

		// sequences contained in the schema + sequences used by serials
		sequences = append(sequences, schema.Sequences...)
		if len(sequences) > 0 {
			ofs.WriteSql(GlobalSequence.GetShimCreationSql()...)
			ofs.WriteSql(GlobalSequence.GetMultiCreationSql(schema, sequences)...)
			// NOTE: v1 code iterates through all grants here, although any grant applies to all grants.
			// v2+ simplifies this to generate grants holistically
			ofs.WriteSql(GlobalSequence.GetMultiGrantSql(doc, schema, sequences)...)
		}

		// trigger definitions + triggers used by serials
		triggers = append(triggers, schema.Triggers...)
		uniqueTriggers := map[string]string{}
		for _, trigger := range triggers {
			if trigger.SqlFormat.Equals(model.SqlFormatMysql5) {
				// check that this table/timing/event combo hasn't been defined,
				// because MySQL only allows one trigger per BEFORE/AFTER per action
				// TODO(go,mysql) confirm this handling of .Events works as expected
				uniqueName := fmt.Sprintf("%s-%s-%s", trigger.Table, trigger.Timing, strings.Join(trigger.Events, ";"))
				if collision, hasCollision := uniqueTriggers[uniqueName]; hasCollision {
					dbsteward.Fatal(
						"MySQL will not allow trigger '%s'.'%s' (%s %s) to be created because it happens on the same table/time/event as trigger '%s'",
						schema.Name,
						trigger.Name,
						trigger.Timing,
						strings.Join(trigger.Events, "/"),
						collision,
					)
				}
				uniqueTriggers[uniqueName] = trigger.Name
				ofs.WriteSql(GlobalTrigger.GetCreationSql(schema, trigger)...)
			}
		}
	}

	for _, schema := range doc.Schemas {
		// define table primary keys before foreign keys so unique requirements are always met for FOREIGN KEY constraints
		for _, table := range schema.Tables {
			GlobalDiffConstraints.CreateConstraintsTable(ofs, nil, nil, schema, table, sql99.ConstraintTypePrimaryKey)
		}
	}

	// foreign key references
	// use the dependency order to specify foreign keys in an order that will satisfy nested foreign keys and etc
	// TODO(feat) shouldn't this consider GlobalDBSteward.LimitToTables like BuildData does?
	for _, entry := range tableDep {
		GlobalDiffConstraints.CreateConstraintsTable(ofs, nil, nil, entry.Schema, entry.Table, sql99.ConstraintTypeConstraint)
	}

	GlobalDiffViews.CreateViewsOrdered(ofs, nil, doc)

	// view permission grants
	for _, schema := range doc.Schemas {
		for _, view := range schema.Views {
			for _, grant := range view.Grants {
				ofs.WriteSql(GlobalView.GetGrantSql(doc, schema, view, grant)...)
			}
		}
	}

	// TODO(feat) database configurationParameter support
}

func (self *Operations) BuildData(doc *model.Definition, ofs output.OutputFileSegmenter, tableDep []*model.TableRef) {
	// TODO(go,3) unify this with pgsql implementation?
	limitToTables := lib.GlobalDBSteward.LimitToTables

	// use the dependency order to then write out the actual data inserts into the data sql file
	for _, entry := range tableDep {
		schema := entry.Schema
		table := entry.Table

		// skip any tables that are not in the limit list, if there are any tables to limit
		if len(limitToTables) > 0 {
			if includeTables, ok := limitToTables[schema.Name]; ok {
				if !util.InArrayStr(table.Name, includeTables) {
					continue
				}
			} else {
				// if this entry's schema didn't appear in the include list, we can't possibly include any tables from it
				continue
			}
		}

		ofs.WriteSql(GlobalDiffTables.GetCreateDataSql(nil, nil, schema, table)...)

		// set serial primary keys to the max value after inserts have been performed
		// only if the PRIMARY KEY is not a multi column
		// TODO(go,4) move sequence polyfill behind a flag
		if table.Rows != nil && len(table.PrimaryKey) == 1 {
			dataCols := table.Rows.Columns
			pkCol := table.PrimaryKey[0]
			if util.InArrayStr(pkCol, dataCols) {
				// TODO(go,3) seems like this could be refactored better by putting much of the lookup
				// into the model structs
				pk := lib.GlobalDBX.TryInheritanceGetColumn(doc, schema, table, pkCol)
				if pk == nil {
					lib.GlobalDBSteward.Fatal("Failed to find primary key column '%s' for %s.%s",
						pkCol, schema.Name, table.Name)
				}
				// only set the pkey to MAX() if the primary key column is also a serial/bigserial and if serialStart is not defined
				// TODO(go,nth) unify DataType.IsLinkedType and Column.IsSerialType
				if GlobalColumn.IsSerialType(pk) && pk.SerialStart == nil {
					ofs.WriteSql(&sql.SequenceSerialSetValMax{
						Column: sql.ColumnRef{schema.Name, table.Name, pk.Name},
					})
				}
			}
		}

		// unlike the pg class, we cannot just set identity column start values here with setval without inserting a row
		// check if primary key is a column of this table - FS#17481
		for _, columnName := range table.PrimaryKey {
			col := lib.GlobalDBX.TryInheritanceGetColumn(doc, schema, table, columnName)
			if col == nil {
				lib.GlobalDBSteward.Fatal("Declared primary key column (%s) does not exist as column in table %s.%s",
					columnName, schema.Name, table.Name)
			}
		}
	}

	// include all of the unstaged sql elements
	lib.GlobalDBX.BuildStagedSql(doc, ofs, "")
}

func (self *Operations) BuildUpgrade(
	oldOutputPrefix string, oldCompositeFile string, oldDoc *model.Definition, oldFiles []string,
	newOutputPrefix string, newCompositeFile string, newDoc *model.Definition, newFiles []string,
) {
	upgradePrefix := newOutputPrefix + "_upgrade"

	lib.GlobalDBSteward.Info("Calculating old table foreign key dependency order...")
	GlobalDiff.OldTableDependency = lib.GlobalDBX.TableDependencyOrder(oldDoc)

	lib.GlobalDBSteward.Info("Calculating new table foreign key dependency order...")
	GlobalDiff.NewTableDependency = lib.GlobalDBX.TableDependencyOrder(newDoc)

	GlobalDiff.DiffDoc(oldCompositeFile, newCompositeFile, oldDoc, newDoc, upgradePrefix)
}

func (self *Operations) ExtractSchema(host string, port uint, name, user, pass string) *model.Definition {
	// TODO(go,mysql) implement me; see mysql5::extract_schema
	return nil
}

func (self *Operations) GetQuoter() output.Quoter {
	// TODO(go,core) why is this part of public interface? can it not be?
	// TODO(go,mysql) implement me
	return nil
}
