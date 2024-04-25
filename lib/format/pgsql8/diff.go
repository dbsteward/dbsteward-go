package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/format/sql99"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

type diff struct {
	*sql99.Diff
	OldTableDependency []*ir.TableRef
	NewTableDependency []*ir.TableRef
}

func newDiff() *diff {
	diff := &diff{
		Diff: sql99.NewDiff(GlobalLookup),
	}
	diff.Diff.Diff = diff
	return diff
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

func (d *diff) DiffDoc(oldFile, newFile string, oldDoc, newDoc *ir.Definition, upgradePrefix string) {
	if !lib.GlobalDBSteward.GenerateSlonik {
		// if we are not generating slonik, defer to parent
		d.Diff.DiffDoc(oldFile, newFile, oldDoc, newDoc, upgradePrefix)
		return
	}

	// TODO(go,slony)
}

func (d *diff) DiffDocWork(stage1, stage2, stage3, stage4 output.OutputFileSegmenter) {
	dbsteward := lib.GlobalDBSteward
	dbx := lib.GlobalDBX

	// this shouldn't be called if we're not generating slonik, it looks for
	// a slony element in <database> which most likely won't be there if
	// we're not interested in slony replication
	if dbsteward.GenerateSlonik {
		// TODO(go,slony)
	}

	// stage 1 and 3 should not be in a transaction as they will be submitted via slonik EXECUTE SCRIPT
	if !dbsteward.GenerateSlonik {
		stage1.AppendHeader("\nBEGIN;\n\n")
		stage1.AppendFooter("\nCOMMIT;\n")
	} else {
		stage1.AppendHeader("\n-- generateslonik specified: pgsql8 STAGE1 upgrade omitting BEGIN. slonik EXECUTE SCRIPT will wrap stage 1 DDL and DCL in a transaction\n")
	}

	if !dbsteward.SingleStageUpgrade {
		stage2.AppendHeader("\nBEGIN;\n\n")
		stage2.AppendFooter("\nCOMMIT;\n")
		stage4.AppendHeader("\nBEGIN;\n\n")
		stage4.AppendFooter("\nCOMMIT;\n")

		// stage 1 and 3 should not be in a transaction as they will be submitted via slonik EXECUTE SCRIPT
		if !dbsteward.GenerateSlonik {
			stage3.AppendHeader("\nBEGIN;\n\n")
			stage3.AppendFooter("\nCOMMIT;\n")
		} else {
			stage3.AppendHeader("\n-- generateslonik specified: pgsql8 STAGE3 upgrade omitting BEGIN. slonik EXECUTE SCRIPT will wrap stage 3 DDL and DCL in a transaction\n")
		}
	}

	// start with pre-upgrade sql statements that prepare the database to take on its changes
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage1, "STAGE1BEFORE")
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage2, "STAGE2BEFORE")

	dbsteward.Info("Drop Old Schemas")
	d.DropOldSchemas(stage3)

	dbsteward.Info("Create New Schemas")
	d.CreateNewSchemas(stage1)

	dbsteward.Info("Update Structure")
	d.updateStructure(stage1, stage3)

	dbsteward.Info("Update Permissions")
	d.updatePermissions(stage1, stage3)

	d.UpdateDatabaseConfigParameters(stage1, dbsteward.NewDatabase, dbsteward.OldDatabase)

	dbsteward.Info("Update data")
	if dbsteward.GenerateSlonik {
		// TODO(go,slony) format::set_context_replica_set_to_natural_first(dbsteward::$new_database);
	}
	d.updateData(stage2, true)
	d.updateData(stage4, false)

	// append any literal sql in new not in old at the end of data stage 1
	// TODO(feat) this relies on exact string match - is there a better way?
	for _, newSql := range dbsteward.NewDatabase.Sql {
		// ignore upgrade staged sql elements
		if newSql.Stage != "" {
			continue
		}

		found := false
		for _, oldSql := range dbsteward.OldDatabase.Sql {
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
			stage2.Write(newSql.Text + "\n")
		}
	}

	// append stage defined sql statements to appropriate stage file
	if dbsteward.GenerateSlonik {
		// TODO(go,slony) format::set_context_replica_set_to_natural_first(dbsteward::$new_database);
	}

	dbx.BuildStagedSql(dbsteward.NewDatabase, stage1, "STAGE1")
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage2, "STAGE2")
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage3, "STAGE3")
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage4, "STAGE4")
}

func (d *diff) DiffSql(old, new []string, upgradePrefix string) {
	// TODO(go,sqldiff)
}

func (d *diff) updateStructure(stage1 output.OutputFileSegmenter, stage3 output.OutputFileSegmenter) {
	dbsteward := lib.GlobalDBSteward

	diffLanguages(stage1)

	// drop all views in all schemas, regardless whether dependency order is known or not
	// TODO(go,4) would be so cool if we could parse the view def and only recreate what's required
	dropViewsOrdered(stage1, dbsteward.OldDatabase, dbsteward.NewDatabase)

	// TODO(go,3) should we just always use table deps?
	if len(d.NewTableDependency) == 0 {
		for _, newSchema := range dbsteward.NewDatabase.Schemas {
			oldSchema := dbsteward.OldDatabase.TryGetSchemaNamed(newSchema.Name)
			diffTypes(stage1, oldSchema, newSchema)
			diffFunctions(stage1, stage3, oldSchema, newSchema)
			diffSequences(stage1, oldSchema, newSchema)
			// remove old constraints before table constraints, so the sql statements succeed
			dropConstraints(stage1, oldSchema, newSchema, sql99.ConstraintTypeConstraint)
			dropConstraints(stage1, oldSchema, newSchema, sql99.ConstraintTypePrimaryKey)
			dropTables(stage1, oldSchema, newSchema)
			err := createTables(stage1, oldSchema, newSchema)
			dbsteward.FatalIfError(err, "while creating tables")
			err = diffTables(stage1, stage3, oldSchema, newSchema)
			dbsteward.FatalIfError(err, "while diffing tables")
			diffIndexes(stage1, oldSchema, newSchema)
			diffClusters(stage1, oldSchema, newSchema)
			createConstraints(stage1, oldSchema, newSchema, sql99.ConstraintTypePrimaryKey)
			diffTriggers(stage1, oldSchema, newSchema)
		}
		// non-primary key constraints may be inter-schema dependant, and dependant on other's primary keys
		// and therefore should be done after object creation sections
		for _, newSchema := range dbsteward.NewDatabase.Schemas {
			oldSchema := dbsteward.OldDatabase.TryGetSchemaNamed(newSchema.Name)
			createConstraints(stage1, oldSchema, newSchema, sql99.ConstraintTypeConstraint)
		}
	} else {
		// use table dependency order to do structural changes in an intelligent order
		// make sure we only process each schema once
		processedSchemas := map[string]bool{}
		for _, newEntry := range d.NewTableDependency {
			newSchema := newEntry.Schema
			oldSchema := dbsteward.OldDatabase.TryGetSchemaNamed(newSchema.Name)

			if !processedSchemas[newSchema.Name] {
				diffTypes(stage1, oldSchema, newSchema)
				diffFunctions(stage1, stage3, oldSchema, newSchema)
				processedSchemas[newSchema.Name] = true
			}
		}

		// remove all old constraints before new contraints, in reverse dependency order
		// TODO(go,pgsql) REVERSE dependency order
		for _, oldEntry := range d.OldTableDependency {
			oldSchema := oldEntry.Schema
			oldTable := oldEntry.Table

			newSchema := dbsteward.NewDatabase.TryGetSchemaNamed(oldSchema.Name)
			var newTable *ir.Table
			if newSchema != nil {
				newTable = newSchema.TryGetTableNamed(oldTable.Name)
			}

			// NOTE: when dropping constraints, GlobalDBX.RenamedTableCheckPointer() is not called for oldTable
			// as GlobalDiffConstraints.DiffConstraintsTable() will do rename checking when recreating constraints for renamed tables
			dropConstraintsTable(stage1, oldSchema, oldTable, newSchema, newTable, sql99.ConstraintTypeConstraint)
			dropConstraintsTable(stage1, oldSchema, oldTable, newSchema, newTable, sql99.ConstraintTypePrimaryKey)
		}

		processedSchemas = map[string]bool{}
		for _, newEntry := range d.NewTableDependency {
			newSchema := newEntry.Schema
			oldSchema := dbsteward.OldDatabase.TryGetSchemaNamed(newSchema.Name)

			// schema level stuff should only be done once, keep track of which ones we have done
			// see above for pre table creation stuff
			// see below for post table creation stuff
			if !processedSchemas[newSchema.Name] {
				diffSequences(stage1, oldSchema, newSchema)
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
			oldSchema, oldTable = lib.GlobalDBX.RenamedTableCheckPointer(oldSchema, oldTable, newSchema, newTable)
			err := createTable(stage1, oldSchema, newSchema, newTable)
			lib.GlobalDBSteward.FatalIfError(err, "while creating table %s.%s", newSchema.Name, newTable.Name)
			err = diffTable(stage1, stage3, oldSchema, oldTable, newSchema, newTable)
			lib.GlobalDBSteward.FatalIfError(err, "while diffing table %s.%s", newSchema.Name, newTable.Name)
			diffIndexesTable(stage1, oldSchema, oldTable, newSchema, newTable)
			diffClustersTable(stage1, oldTable, newSchema, newTable)
			createConstraintsTable(stage1, oldSchema, oldTable, newSchema, newTable, sql99.ConstraintTypePrimaryKey)
			diffTriggersTable(stage1, oldSchema, oldTable, newSchema, newTable)

			// HACK: For now, we'll generate foreign key constraints in stage 4 in updateData below
			// https://github.com/dbsteward/dbsteward/issues/142
			createConstraintsTable(stage1, oldSchema, oldTable, newSchema, newTable, sql99.ConstraintTypeConstraint&^sql99.ConstraintTypeForeign)
		}

		// drop old tables in reverse dependency order
		for i := len(d.OldTableDependency) - 1; i >= 0; i -= 1 {
			oldEntry := d.OldTableDependency[i]
			oldSchema := oldEntry.Schema
			oldTable := oldEntry.Table

			newSchema := dbsteward.NewDatabase.TryGetSchemaNamed(oldSchema.Name)
			dropTable(stage3, oldSchema, oldTable, newSchema)
		}
	}

	createViewsOrdered(stage3, dbsteward.OldDatabase, dbsteward.NewDatabase)
}

func (d *diff) updatePermissions(stage1 output.OutputFileSegmenter, stage3 output.OutputFileSegmenter) {
	// TODO(feat) what if readonly user changed? we need to rebuild those grants
	// TODO(feat) what about removed permissions, shouldn't we REVOKE those?

	newDoc := lib.GlobalDBSteward.NewDatabase
	oldDoc := lib.GlobalDBSteward.OldDatabase
	for _, newSchema := range newDoc.Schemas {
		oldSchema := oldDoc.TryGetSchemaNamed(newSchema.Name)

		for _, newGrant := range newSchema.Grants {
			if oldSchema == nil || !ir.HasPermissionsOf(oldSchema, newGrant, ir.SqlFormatPgsql8) {
				stage1.WriteSql(GlobalSchema.GetGrantSql(newDoc, newSchema, newGrant)...)
			}
		}

		for _, newTable := range newSchema.Tables {
			oldTable := oldSchema.TryGetTableNamed(newTable.Name)
			isRenamed, err := lib.GlobalDBX.IsRenamedTable(newSchema, newTable)
			lib.GlobalDBSteward.FatalIfError(err, "while updating permissions")
			if isRenamed {
				// skip permission diffing on it, it is the same
				// TODO(feat) that seems unlikely? we should probably check permissions on renamed table
				continue
			}
			for _, newGrant := range newTable.Grants {
				if oldTable == nil || !ir.HasPermissionsOf(oldTable, newGrant, ir.SqlFormatPgsql8) {
					stage1.WriteSql(getTableGrantSql(newSchema, newTable, newGrant)...)
				}
			}
		}

		for _, newSeq := range newSchema.Sequences {
			oldSeq := oldSchema.TryGetSequenceNamed(newSeq.Name)
			for _, newGrant := range newSeq.Grants {
				if oldSeq == nil || !ir.HasPermissionsOf(oldSeq, newGrant, ir.SqlFormatPgsql8) {
					stage1.WriteSql(getSequenceGrantSql(newDoc, newSchema, newSeq, newGrant)...)
				}
			}
		}

		for _, newFunc := range newSchema.Functions {
			oldFunc := oldSchema.TryGetFunctionMatching(newFunc)
			for _, newGrant := range newFunc.Grants {
				if oldFunc == nil || !ir.HasPermissionsOf(oldFunc, newGrant, ir.SqlFormatPgsql8) {
					stage1.WriteSql(getFunctionGrantSql(newSchema, newFunc, newGrant)...)
				}
			}
		}

		for _, newView := range newSchema.Views {
			oldView := oldSchema.TryGetViewNamed(newView.Name)
			for _, newGrant := range newView.Grants {
				if lib.GlobalDBSteward.AlwaysRecreateViews || oldView == nil || !ir.HasPermissionsOf(oldView, newGrant, ir.SqlFormatPgsql8) || !oldView.Equals(newView, ir.SqlFormatPgsql8) {
					stage3.WriteSql(getViewGrantSql(newDoc, newSchema, newView, newGrant)...)
				}
			}
		}
	}
}
func (d *diff) updateData(ofs output.OutputFileSegmenter, deleteMode bool) {
	if len(d.NewTableDependency) > 0 {
		for i := 0; i < len(d.NewTableDependency); i += 1 {
			item := d.NewTableDependency[i]
			// go in reverse when in delete mode
			if deleteMode {
				item = d.NewTableDependency[len(d.NewTableDependency)-1-i]
			}

			newSchema := item.Schema
			newTable := item.Table
			oldSchema := lib.GlobalDBSteward.OldDatabase.TryGetSchemaNamed(newSchema.Name)
			oldTable := oldSchema.TryGetTableNamed(newTable.Name)

			isRenamed, err := lib.GlobalDBX.IsRenamedTable(newSchema, newTable)
			lib.GlobalDBSteward.FatalIfError(err, "while updating data")
			if isRenamed {
				lib.GlobalDBSteward.Info("%s.%s used to be called %s - will diff data against that definition", newSchema.Name, newTable.Name, newTable.OldTableName)
				oldSchema = lib.GlobalDBX.GetOldTableSchema(newSchema, newTable)
				oldTable = lib.GlobalDBX.GetOldTable(newSchema, newTable)
			}

			if deleteMode {
				// TODO(go,3) clean up inconsistencies between e.g. GetDeleteDataSql and DiffData wrt writing sql to an ofs
				// TODO(feat) aren't deletes supposed to go in stage 2?
				ofs.WriteSql(getDeleteDataSql(oldSchema, oldTable, newSchema, newTable)...)
			} else {
				ofs.WriteSql(getCreateDataSql(oldSchema, oldTable, newSchema, newTable)...)

				// HACK: For now, we'll generate foreign key constraints in stage 4 after inserting data
				// https://github.com/dbsteward/dbsteward/issues/142
				createConstraintsTable(ofs, oldSchema, oldTable, newSchema, newTable, sql99.ConstraintTypeForeign)
			}
		}
	} else {
		// dependency order unknown, hit them in natural order
		// TODO(feat) the above switches on deleteMode, this does not. we never delete data if table dep order is unknown?
		for _, newSchema := range lib.GlobalDBSteward.NewDatabase.Schemas {
			oldSchema := lib.GlobalDBSteward.OldDatabase.TryGetSchemaNamed(newSchema.Name)
			diffData(ofs, oldSchema, newSchema)
		}
	}
}
