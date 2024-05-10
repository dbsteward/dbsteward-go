package pgsql8

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/format/sql99"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

type diff struct {
	quoter             output.Quoter
	ops                *Operations
	OldTableDependency []*ir.TableRef
	NewTableDependency []*ir.TableRef
}

func newDiff(ops *Operations, q output.Quoter) *diff {
	return &diff{
		ops:    ops,
		quoter: q,
	}
}

func (d *diff) Quoter() output.Quoter {
	return d.quoter
}

func (d *diff) UpdateDatabaseConfigParameters(ofs output.OutputFileSegmenter, oldDoc *ir.Definition, newDoc *ir.Definition) {
	if newDoc.Database == nil {
		newDoc.Database = &ir.Database{}
	}
	for _, newParam := range newDoc.Database.ConfigParams {
		var oldParam *ir.ConfigParam
		if oldDoc != nil {
			if oldDoc.Database == nil {
				oldDoc.Database = &ir.Database{}
			}
			oldParam = oldDoc.Database.TryGetConfigParamNamed(newParam.Name)
		}
		oldValue := "not defined"
		if oldParam != nil {
			oldValue = oldParam.Value
		}
		if oldParam == nil || !oldParam.Equals(newParam) {
			ofs.WriteSql(&sql.Annotated{
				Wrapped: &sql.ConfigParamSet{
					Name:  newParam.Name,
					Value: newParam.Value,
				},
				Annotation: "old configurationParameter value: " + oldValue,
			})
		}
	}
}

func (d *diff) DiffDocWork(stage1, stage2, stage3, stage4 output.OutputFileSegmenter) error {

	// this shouldn't be called if we're not generating slonik, it looks for
	// a slony element in <database> which most likely won't be there if
	// we're not interested in slony replication
	if d.ops.config.GenerateSlonik {
		// TODO(go,slony)
	}

	// stage 1 and 3 should not be in a transaction as they will be submitted via slonik EXECUTE SCRIPT
	if !d.ops.config.GenerateSlonik {
		stage1.AppendHeader(output.NewRawSQL("\nBEGIN;\n\n"))
		stage1.AppendFooter(output.NewRawSQL("\nCOMMIT;\n"))
	} else {
		stage1.AppendHeader(sql.NewComment("generateslonik specified: pgsql8 STAGE1 upgrade omitting BEGIN. slonik EXECUTE SCRIPT will wrap stage 1 DDL and DCL in a transaction"))
	}

	if !d.ops.config.SingleStageUpgrade {
		stage2.AppendHeader(output.NewRawSQL("\nBEGIN;\n\n"))
		stage2.AppendFooter(output.NewRawSQL("\nCOMMIT;\n"))
		stage4.AppendHeader(output.NewRawSQL("\nBEGIN;\n\n"))
		stage4.AppendFooter(output.NewRawSQL("\nCOMMIT;\n"))

		// stage 1 and 3 should not be in a transaction as they will be submitted via slonik EXECUTE SCRIPT
		if !d.ops.config.GenerateSlonik {
			stage3.AppendHeader(output.NewRawSQL("\nBEGIN;\n\n"))
			stage3.AppendFooter(output.NewRawSQL("\nCOMMIT;\n"))
		} else {
			stage3.AppendHeader(sql.NewComment("generateslonik specified: pgsql8 STAGE3 upgrade omitting BEGIN. slonik EXECUTE SCRIPT will wrap stage 3 DDL and DCL in a transaction"))
		}
	}

	// start with pre-upgrade sql statements that prepare the database to take on its changes
	buildStagedSql(d.ops.config.NewDatabase, stage1, "STAGE1BEFORE")
	buildStagedSql(d.ops.config.NewDatabase, stage2, "STAGE2BEFORE")

	d.ops.config.Logger.Info("Drop Old Schemas")
	d.DropOldSchemas(stage3)

	d.ops.config.Logger.Info("Create New Schemas")
	err := d.CreateNewSchemas(stage1)
	if err != nil {
		return err
	}

	err = d.updateStructure(stage1, stage3)
	if err != nil {
		return err
	}

	d.ops.config.Logger.Info("Update Permissions")
	err = d.updatePermissions(stage1, stage3)
	if err != nil {
		return err
	}

	d.UpdateDatabaseConfigParameters(stage1, d.ops.config.NewDatabase, d.ops.config.OldDatabase)

	d.ops.config.Logger.Info("Update data")
	if d.ops.config.GenerateSlonik {
		// TODO(go,slony) format::set_context_replica_set_to_natural_first(dbsteward::$new_database);
	}
	err = d.updateData(stage2, true)
	if err != nil {
		return err
	}
	err = d.updateData(stage4, false)
	if err != nil {
		return err
	}

	// append any literal sql in new not in old at the end of data stage 1
	// TODO(feat) this relies on exact string match - is there a better way?
	for _, newSql := range d.ops.config.NewDatabase.Sql {
		// ignore upgrade staged sql elements
		if newSql.Stage != "" {
			continue
		}

		found := false
		for _, oldSql := range d.ops.config.OldDatabase.Sql {
			// ignore upgrade staged sql elements
			if oldSql.Stage != "" {
				continue
			}
			if newSql.Text == oldSql.Text {
				found = true
				break
			}
		}

		if !found {
			// TODO(feat) need to ensure newSql ends with semicolon
			stage2.WriteSql(output.NewRawSQL(newSql.Text + "\n"))
		}
	}

	// append stage defined sql statements to appropriate stage file
	if d.ops.config.GenerateSlonik {
		// TODO(go,slony) format::set_context_replica_set_to_natural_first(dbsteward::$new_database);
	}

	buildStagedSql(d.ops.config.NewDatabase, stage1, "STAGE1")
	buildStagedSql(d.ops.config.NewDatabase, stage2, "STAGE2")
	buildStagedSql(d.ops.config.NewDatabase, stage3, "STAGE3")
	buildStagedSql(d.ops.config.NewDatabase, stage4, "STAGE4")
	return nil
}

func (d *diff) DiffSql(old, new []string, upgradePrefix string) {
	// TODO(go,sqldiff)
}

func (d *diff) updateStructure(stage1 output.OutputFileSegmenter, stage3 output.OutputFileSegmenter) error {
	logger := d.ops.config.Logger
	logger.Info("Update Structure")

	err := diffLanguages(d.ops.config, stage1)
	if err != nil {
		return err
	}

	// drop all views in all schemas, regardless whether dependency order is known or not
	// TODO(go,4) would be so cool if we could parse the view def and only recreate what's required
	dropViewsOrdered(stage1, d.ops.config.OldDatabase, d.ops.config.NewDatabase)

	// TODO(go,3) should we just always use table deps?
	if len(d.NewTableDependency) == 0 {
		logger.Debug("not using table dependencies")
		for _, newSchema := range d.ops.config.NewDatabase.Schemas {
			oldSchema := d.ops.config.OldDatabase.TryGetSchemaNamed(newSchema.Name)
			err := diffTypes(d.ops.config, d, stage1, oldSchema, newSchema)
			if err != nil {
				return err
			}
			err = diffFunctions(d.ops.config, stage1, stage3, oldSchema, newSchema)
			if err != nil {
				return err
			}
			err = diffSequences(d.ops.config, stage1, oldSchema, newSchema)
			if err != nil {
				return fmt.Errorf("while diffing sequences: %w", err)
			}
			// remove old constraints before table constraints, so the sql statements succeed
			err = dropConstraints(d.ops.config, stage1, oldSchema, newSchema, sql99.ConstraintTypeConstraint)
			if err != nil {
				return err
			}
			err = dropConstraints(d.ops.config, stage1, oldSchema, newSchema, sql99.ConstraintTypePrimaryKey)
			if err != nil {
				return err
			}
			dropTables(d.ops.config, stage1, oldSchema, newSchema)
			err = createTables(d.ops.config, stage1, oldSchema, newSchema)
			if err != nil {
				return fmt.Errorf("while creating tables: %w", err)
			}
			err = diffTables(d.ops.config, stage1, stage3, oldSchema, newSchema)
			if err != nil {
				return fmt.Errorf("while diffing tables: %w", err)
			}
			err = diffIndexes(stage1, oldSchema, newSchema)
			if err != nil {
				return err
			}
			diffClusters(stage1, oldSchema, newSchema)
			createConstraints(d.ops.config, stage1, oldSchema, newSchema, sql99.ConstraintTypePrimaryKey)
			err = diffTriggers(stage1, oldSchema, newSchema)
			if err != nil {
				return err
			}
		}
		// non-primary key constraints may be inter-schema dependant, and dependant on other's primary keys
		// and therefore should be done after object creation sections
		for _, newSchema := range d.ops.config.NewDatabase.Schemas {
			oldSchema := d.ops.config.OldDatabase.TryGetSchemaNamed(newSchema.Name)
			createConstraints(d.ops.config, stage1, oldSchema, newSchema, sql99.ConstraintTypeConstraint)
		}
	} else {
		logger.Debug("using table dependencies")
		// use table dependency order to do structural changes in an intelligent order
		// make sure we only process each schema once
		processedSchemas := map[string]bool{}
		for _, newEntry := range d.NewTableDependency {
			newSchema := newEntry.Schema
			oldSchema := d.ops.config.OldDatabase.TryGetSchemaNamed(newSchema.Name)

			if !processedSchemas[newSchema.Name] {
				err := diffTypes(d.ops.config, d, stage1, oldSchema, newSchema)
				if err != nil {
					return err
				}
				err = diffFunctions(d.ops.config, stage1, stage3, oldSchema, newSchema)
				if err != nil {
					return err
				}
				processedSchemas[newSchema.Name] = true
			}
		}

		// remove all old constraints before new contraints, in reverse dependency order
		// TODO(go,pgsql) REVERSE dependency order
		for _, oldEntry := range d.OldTableDependency {
			oldSchema := oldEntry.Schema
			oldTable := oldEntry.Table

			newSchema := d.ops.config.NewDatabase.TryGetSchemaNamed(oldSchema.Name)
			var newTable *ir.Table
			if newSchema != nil {
				newTable = newSchema.TryGetTableNamed(oldTable.Name)
			}

			// NOTE: when dropping constraints, GlobalDBX.RenamedTableCheckPointer() is not called for oldTable
			// as GlobalDiffConstraints.DiffConstraintsTable() will do rename checking when recreating constraints for renamed tables
			err := dropConstraintsTable(d.ops.config, stage1, oldSchema, oldTable, newSchema, newTable, sql99.ConstraintTypeConstraint)
			if err != nil {
				return err
			}
			err = dropConstraintsTable(d.ops.config, stage1, oldSchema, oldTable, newSchema, newTable, sql99.ConstraintTypePrimaryKey)
			if err != nil {
				return err
			}
		}

		processedSchemas = map[string]bool{}
		for _, newEntry := range d.NewTableDependency {
			newSchema := newEntry.Schema
			oldSchema := d.ops.config.OldDatabase.TryGetSchemaNamed(newSchema.Name)

			// schema level stuff should only be done once, keep track of which ones we have done
			// see above for pre table creation stuff
			// see below for post table creation stuff
			if !processedSchemas[newSchema.Name] {
				err := diffSequences(d.ops.config, stage1, oldSchema, newSchema)
				if err != nil {
					return fmt.Errorf("while diffing sequences: %w", err)
				}
				processedSchemas[newSchema.Name] = true
			}

			newTable := newEntry.Table
			var oldTable *ir.Table
			if oldSchema != nil {
				oldTable = oldSchema.TryGetTableNamed(newTable.Name)
			}

			// if they are defined in the old definition,
			// oldSchema and oldTable are already established pointers
			// when a table has an oldTableName oldSchemaName specified,
			// GlobalDBX.RenamedTableCheckPointer() will modify these pointers to be the old table
			var err error
			oldSchema, oldTable, err = d.ops.config.OldDatabase.NewTableName(oldSchema, oldTable, newSchema, newTable)
			if err != nil {
				return fmt.Errorf("getting new table name: %w", err)
			}
			err = createTable(d.ops.config, stage1, oldSchema, newSchema, newTable)
			if err != nil {
				return fmt.Errorf("while creating table %s.%s: %w", newSchema.Name, newTable.Name, err)
			}
			err = diffTable(d.ops.config, stage1, stage3, oldSchema, oldTable, newSchema, newTable)
			if err != nil {
				return fmt.Errorf("while diffing table %s.%s: %w", newSchema.Name, newTable.Name, err)
			}
			err = diffIndexesTable(stage1, oldSchema, oldTable, newSchema, newTable)
			if err != nil {
				return err
			}
			diffClustersTable(stage1, oldTable, newSchema, newTable)
			err = createConstraintsTable(d.ops.config, stage1, oldSchema, oldTable, newSchema, newTable, sql99.ConstraintTypePrimaryKey)
			if err != nil {
				return err
			}
			err = diffTriggersTable(stage1, oldSchema, oldTable, newSchema, newTable)
			if err != nil {
				return err
			}

			// HACK: For now, we'll generate foreign key constraints in stage 4 in updateData below
			// https://github.com/dbsteward/dbsteward/issues/142
			err = createConstraintsTable(d.ops.config, stage1, oldSchema, oldTable, newSchema, newTable, sql99.ConstraintTypeConstraint&^sql99.ConstraintTypeForeign)
			if err != nil {
				return err
			}
		}

		// drop old tables in reverse dependency order
		for i := len(d.OldTableDependency) - 1; i >= 0; i -= 1 {
			oldEntry := d.OldTableDependency[i]
			oldSchema := oldEntry.Schema
			oldTable := oldEntry.Table

			newSchema := d.ops.config.NewDatabase.TryGetSchemaNamed(oldSchema.Name)
			dropTable(d.ops.config, stage3, oldSchema, oldTable, newSchema)
		}
	}

	return createViewsOrdered(d.ops.config, stage3, d.ops.config.OldDatabase, d.ops.config.NewDatabase)
}

func (d *diff) updatePermissions(stage1 output.OutputFileSegmenter, stage3 output.OutputFileSegmenter) error {
	// TODO(feat) what if readonly user changed? we need to rebuild those grants
	// TODO(feat) what about removed permissions, shouldn't we REVOKE those?

	newDoc := d.ops.config.NewDatabase
	oldDoc := d.ops.config.OldDatabase
	for _, newSchema := range newDoc.Schemas {
		oldSchema := oldDoc.TryGetSchemaNamed(newSchema.Name)
		for _, newGrant := range newSchema.Grants {
			if oldSchema == nil || !ir.HasPermissionsOf(oldSchema, newGrant, ir.SqlFormatPgsql8) {
				s, err := commonSchema.GetGrantSql(d.ops.config, newDoc, newSchema, newGrant)
				if err != nil {
					return err
				}
				stage1.WriteSql(s...)
			}
		}

		for _, newTable := range newSchema.Tables {
			oldTable := oldSchema.TryGetTableNamed(newTable.Name)
			isRenamed, err := d.ops.config.OldDatabase.IsRenamedTable(slog.Default(), newSchema, newTable)
			if err != nil {
				return fmt.Errorf("while updating permissions: %w", err)
			}
			if isRenamed {
				// skip permission diffing on it, it is the same
				// TODO(feat) that seems unlikely? we should probably check permissions on renamed table
				continue
			}
			for _, newGrant := range newTable.Grants {
				if oldTable == nil || !ir.HasPermissionsOf(oldTable, newGrant, ir.SqlFormatPgsql8) {
					s, err := getTableGrantSql(d.ops.config, newSchema, newTable, newGrant)
					if err != nil {
						return err
					}
					stage1.WriteSql(s...)
				}
			}
		}

		for _, newSeq := range newSchema.Sequences {
			oldSeq := oldSchema.TryGetSequenceNamed(newSeq.Name)
			for _, newGrant := range newSeq.Grants {
				if oldSeq == nil || !ir.HasPermissionsOf(oldSeq, newGrant, ir.SqlFormatPgsql8) {
					s, err := getSequenceGrantSql(d.ops.config, newSchema, newSeq, newGrant)
					if err != nil {
						return err
					}
					stage1.WriteSql(s...)
				}
			}
		}

		for _, newFunc := range newSchema.Functions {
			oldFunc := oldSchema.TryGetFunctionMatching(newFunc)
			for _, newGrant := range newFunc.Grants {
				if oldFunc == nil || !ir.HasPermissionsOf(oldFunc, newGrant, ir.SqlFormatPgsql8) {
					grants, err := getFunctionGrantSql(d.ops.config, newSchema, newFunc, newGrant)
					if err != nil {
						return err
					}
					stage1.WriteSql(grants...)
				}
			}
		}

		for _, newView := range newSchema.Views {
			oldView := oldSchema.TryGetViewNamed(newView.Name)
			for _, newGrant := range newView.Grants {
				if d.ops.config.AlwaysRecreateViews || oldView == nil || !ir.HasPermissionsOf(oldView, newGrant, ir.SqlFormatPgsql8) || !oldView.Equals(newView, ir.SqlFormatPgsql8) {
					s, err := getViewGrantSql(d.ops.config, newDoc, newSchema, newView, newGrant)
					if err != nil {
						return err
					}
					stage3.WriteSql(s...)
				}
			}
		}
	}
	return nil
}

func (d *diff) updateData(ofs output.OutputFileSegmenter, deleteMode bool) error {
	if len(d.NewTableDependency) > 0 {
		for i := 0; i < len(d.NewTableDependency); i += 1 {
			item := d.NewTableDependency[i]
			// go in reverse when in delete mode
			if deleteMode {
				item = d.NewTableDependency[len(d.NewTableDependency)-1-i]
			}
			l := d.ops.config.Logger.With(slog.String("table", item.String()))
			newSchema := item.Schema
			newTable := item.Table
			oldSchema := d.ops.config.OldDatabase.TryGetSchemaNamed(newSchema.Name)
			oldTable := oldSchema.TryGetTableNamed(newTable.Name)

			isRenamed, err := d.ops.config.OldDatabase.IsRenamedTable(slog.Default(), newSchema, newTable)
			if err != nil {
				return fmt.Errorf("while updatign data: %w", err)
			}
			if isRenamed {
				l.Info(fmt.Sprintf("%s.%s used to be called %s - will diff data against that definition", newSchema.Name, newTable.Name, newTable.OldTableName))
				oldSchema = d.ops.config.OldDatabase.GetOldTableSchema(newSchema, newTable)
				oldTable = d.ops.config.OldDatabase.GetOldTable(newSchema, newTable)
			}

			if deleteMode {
				// TODO(go,3) clean up inconsistencies between e.g. GetDeleteDataSql and DiffData wrt writing sql to an ofs
				// TODO(feat) aren't deletes supposed to go in stage 2?
				s, err := getDeleteDataSql(d.ops, oldSchema, oldTable, newSchema, newTable)
				if err != nil {
					return err
				}
				ofs.WriteSql(s...)
			} else {
				s, err := getCreateDataSql(d.ops, oldSchema, oldTable, newSchema, newTable)
				if err != nil {
					return err
				}
				ofs.WriteSql(s...)

				// HACK: For now, we'll generate foreign key constraints in stage 4 after inserting data
				// https://github.com/dbsteward/dbsteward/issues/142
				err = createConstraintsTable(d.ops.config, ofs, oldSchema, oldTable, newSchema, newTable, sql99.ConstraintTypeForeign)
				if err != nil {
					return err
				}
			}
		}
	} else {
		// dependency order unknown, hit them in natural order
		// TODO(feat) the above switches on deleteMode, this does not. we never delete data if table dep order is unknown?
		for _, newSchema := range d.ops.config.NewDatabase.Schemas {
			oldSchema := d.ops.config.OldDatabase.TryGetSchemaNamed(newSchema.Name)
			return diffData(d.ops, ofs, oldSchema, newSchema)
		}
	}
	return nil
}

// DropSchemaSQL this implementation is a bit hacky as it's a
// transitional step as I factor away global variables
func (d *diff) DropSchemaSQL(s *ir.Schema) ([]output.ToSql, error) {
	return commonSchema.GetDropSql(s), nil
}

// CreateSchemaSQL this implementation is a bit hacky as it's a
// transitional step as I factor away global variables
func (d *diff) CreateSchemaSQL(s *ir.Schema) ([]output.ToSql, error) {
	return commonSchema.GetCreationSql(d.ops.config, s)
}

func (diff *diff) DiffDoc(oldFile, newFile string, oldDoc, newDoc *ir.Definition, upgradePrefix string) error {
	timestamp := time.Now().Format(time.RFC1123Z)
	oldSetNewSet := fmt.Sprintf("-- Old definition: %s\n-- New definition %s\n", oldFile, newFile)

	var stage1, stage2, stage3, stage4 output.OutputFileSegmenter
	quoter := diff.Quoter()
	logger := diff.ops.config.Logger
	if diff.ops.config.SingleStageUpgrade {
		fileName := upgradePrefix + "_single_stage.sql"
		file, err := os.Create(fileName)
		if err != nil {
			return fmt.Errorf("failed to open %s for write: %w", fileName, err)
		}

		stage1 = output.NewOutputFileSegmenterToFile(logger, quoter, fileName, 1, file, fileName, diff.ops.config.OutputFileStatementLimit)
		stage1.SetHeader(sql.NewComment("DBsteward single stage upgrade changes - generated %s\n%s", timestamp, oldSetNewSet))
		defer stage1.Close()
		stage2 = stage1
		stage3 = stage1
		stage4 = stage1
	} else {
		stage1 = output.NewOutputFileSegmenter(logger, quoter, upgradePrefix+"_stage1_schema", 1, diff.ops.config.OutputFileStatementLimit)
		stage1.SetHeader(sql.NewComment("DBSteward stage 1 structure additions and modifications - generated %s\n%s", timestamp, oldSetNewSet))
		defer stage1.Close()
		stage2 = output.NewOutputFileSegmenter(logger, quoter, upgradePrefix+"_stage2_data", 1, diff.ops.config.OutputFileStatementLimit)
		stage2.SetHeader(sql.NewComment("DBSteward stage 2 data definitions removed - generated %s\n%s", timestamp, oldSetNewSet))
		defer stage2.Close()
		stage3 = output.NewOutputFileSegmenter(logger, quoter, upgradePrefix+"_stage3_schema", 1, diff.ops.config.OutputFileStatementLimit)
		stage3.SetHeader(sql.NewComment("DBSteward stage 3 structure changes, constraints, and removals - generated %s\n%s", timestamp, oldSetNewSet))
		defer stage3.Close()
		stage4 = output.NewOutputFileSegmenter(logger, quoter, upgradePrefix+"_stage4_data", 1, diff.ops.config.OutputFileStatementLimit)
		stage4.SetHeader(sql.NewComment("DBSteward stage 4 data definition changes and additions - generated %s\n%s", timestamp, oldSetNewSet))
		defer stage4.Close()
	}

	diff.ops.config.OldDatabase = oldDoc
	diff.ops.config.NewDatabase = newDoc

	return diff.DiffDocWork(stage1, stage2, stage3, stage4)
}

func (diff *diff) DropOldSchemas(ofs output.OutputFileSegmenter) {
	// TODO(feat) support oldname following?
	for _, oldSchema := range diff.ops.config.OldDatabase.Schemas {
		if diff.ops.config.NewDatabase.TryGetSchemaNamed(oldSchema.Name) == nil {
			diff.ops.config.Logger.Info(fmt.Sprintf("Drop old schema: %s", oldSchema.Name))
			ofs.MustWriteSql(diff.DropSchemaSQL(oldSchema))
		}
	}
}

func (diff *diff) CreateNewSchemas(ofs output.OutputFileSegmenter) error {
	// TODO(feat) support oldname following?
	for _, newSchema := range diff.ops.config.NewDatabase.Schemas {
		if diff.ops.config.OldDatabase.TryGetSchemaNamed(newSchema.Name) == nil {
			diff.ops.config.Logger.Info(fmt.Sprintf("Create new schema: %s", newSchema.Name))
			ofs.MustWriteSql(diff.CreateSchemaSQL(newSchema))
		}
	}
	return nil
}
