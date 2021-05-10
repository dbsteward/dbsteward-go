package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/parse"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/format/sql99"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Diff struct {
	*sql99.Diff
	OldTableDependency []*model.TableRef
	NewTableDependency []*model.TableRef
}

func NewDiff() *Diff {
	diff := &Diff{
		Diff: sql99.NewDiff(GlobalLookup),
	}
	diff.Diff.Diff = diff
	return diff
}

func (self *Diff) UpdateDatabaseConfigParameters(ofs output.OutputFileSegmenter, oldDoc *model.Definition, newDoc *model.Definition) {
	if newDoc.Database == nil {
		newDoc.Database = &model.Database{}
	}
	for _, newParam := range newDoc.Database.ConfigParams {
		var oldParam *model.ConfigParam
		if oldDoc != nil {
			if oldDoc.Database == nil {
				oldDoc.Database = &model.Database{}
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

func (self *Diff) DiffDoc(oldFile, newFile string, oldDoc, newDoc *model.Definition, upgradePrefix string) {
	if !lib.GlobalDBSteward.GenerateSlonik {
		// if we are not generating slonik, defer to parent
		self.Diff.DiffDoc(oldFile, newFile, oldDoc, newDoc, upgradePrefix)
		return
	}

	// TODO(go,slony)
}

func (self *Diff) DiffDocWork(stage1, stage2, stage3, stage4 output.OutputFileSegmenter) {
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
	self.DropOldSchemas(stage3)

	dbsteward.Info("Create New Schemas")
	self.CreateNewSchemas(stage1)

	dbsteward.Info("Update Structure")
	self.updateStructure(stage1, stage3)

	dbsteward.Info("Update Permissions")
	self.updatePermissions(stage1, stage3)

	self.UpdateDatabaseConfigParameters(stage1, dbsteward.NewDatabase, dbsteward.OldDatabase)

	dbsteward.Info("Update data")
	if dbsteward.GenerateSlonik {
		// TODO(go,slony) format::set_context_replica_set_to_natural_first(dbsteward::$new_database);
	}
	self.updateData(stage2, true)
	self.updateData(stage4, false)

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

func (self *Diff) DiffSql(old, new []string, upgradePrefix string) {
	dbsteward := lib.GlobalDBSteward
	xmlParser := lib.GlobalXmlParser

	oldDoc, err := parse.FromFiles(old)
	dbsteward.FatalIfError(err, "Could not parse old sql files")
	oldDoc, err = xmlParser.SqlFormatConvert(oldDoc)
	dbsteward.FatalIfError(err, "Could not make sql format-specific changes to old db structure")
	dbsteward.OldDatabase = oldDoc
	oldFile := upgradePrefix + "_old_sql.xml"
	xmlParser.SaveDoc(oldFile, oldDoc)

	newDoc, err := parse.FromFiles(new)
	dbsteward.FatalIfError(err, "Could not parse new sql files")
	newDoc, err = xmlParser.SqlFormatConvert(newDoc)
	dbsteward.FatalIfError(err, "Could not make sql format-specific changes to new db structure")
	dbsteward.NewDatabase = newDoc
	newFile := upgradePrefix + "_new_sql.xml"
	xmlParser.SaveDoc(newFile, newDoc)

	self.DiffDoc(oldFile, newFile, oldDoc, newDoc, upgradePrefix)
}

func (self *Diff) updateStructure(stage1 output.OutputFileSegmenter, stage3 output.OutputFileSegmenter) {
	dbsteward := lib.GlobalDBSteward

	GlobalDiffLanguages.DiffLanguages(stage1)

	// drop all views in all schemas, regardless whether dependency order is known or not
	// TODO(go,4) would be so cool if we could parse the view def and only recreate what's required
	GlobalDiffViews.DropViewsOrdered(stage1, dbsteward.OldDatabase, dbsteward.NewDatabase)

	// TODO(go,3) should we just always use table deps?
	if len(self.NewTableDependency) == 0 {
		for _, newSchema := range dbsteward.NewDatabase.Schemas {
			GlobalOperations.SetContextReplicaSetId(newSchema.SlonySetId)
			oldSchema := dbsteward.OldDatabase.TryGetSchemaNamed(newSchema.Name)
			GlobalDiffTypes.DiffTypes(stage1, oldSchema, newSchema)
			GlobalDiffFunctions.DiffFunctions(stage1, stage3, oldSchema, newSchema)
			GlobalDiffSequences.DiffSequences(stage1, oldSchema, newSchema)
			// remove old constraints before table constraints, so the sql statements succeed
			GlobalDiffConstraints.DropConstraints(stage1, oldSchema, newSchema, ConstraintTypeConstraint)
			GlobalDiffConstraints.DropConstraints(stage1, oldSchema, newSchema, ConstraintTypePrimaryKey)
			GlobalDiffTables.DropTables(stage1, oldSchema, newSchema)
			err := GlobalDiffTables.CreateTables(stage1, oldSchema, newSchema)
			lib.GlobalDBSteward.FatalIfError(err, "while creating tables")
			err = GlobalDiffTables.DiffTables(stage1, stage3, oldSchema, newSchema)
			lib.GlobalDBSteward.FatalIfError(err, "while diffing tables")
			GlobalDiffIndexes.DiffIndexes(stage1, oldSchema, newSchema)
			GlobalDiffTables.DiffClusters(stage1, oldSchema, newSchema)
			GlobalDiffConstraints.CreateConstraints(stage1, oldSchema, newSchema, ConstraintTypePrimaryKey)
			GlobalDiffTriggers.DiffTriggers(stage1, oldSchema, newSchema)
		}
		// non-primary key constraints may be inter-schema dependant, and dependant on other's primary keys
		// and therefore should be done after object creation sections
		for _, newSchema := range dbsteward.NewDatabase.Schemas {
			GlobalOperations.SetContextReplicaSetId(newSchema.SlonySetId)
			oldSchema := dbsteward.OldDatabase.TryGetSchemaNamed(newSchema.Name)
			GlobalDiffConstraints.CreateConstraints(stage1, oldSchema, newSchema, ConstraintTypeConstraint)
		}
	} else {
		// use table dependency order to do structural changes in an intelligent order
		// make sure we only process each schema once
		processedSchemas := map[string]bool{}
		for _, newEntry := range self.NewTableDependency {
			// NOTE: newEntry.IgnoreEntry is NOT checked here because these are schema operations
			newSchema := newEntry.Schema
			oldSchema := dbsteward.OldDatabase.TryGetSchemaNamed(newSchema.Name)

			if !processedSchemas[newSchema.Name] {
				GlobalOperations.SetContextReplicaSetId(newSchema.SlonySetId)
				GlobalDiffTypes.DiffTypes(stage1, oldSchema, newSchema)
				GlobalDiffFunctions.DiffFunctions(stage1, stage3, oldSchema, newSchema)
				processedSchemas[newSchema.Name] = true
			}
		}

		// remove all old constraints before new contraints, in reverse dependency order
		for _, oldEntry := range self.OldTableDependency {
			oldSchema := oldEntry.Schema
			oldTable := oldEntry.Table

			newSchema := dbsteward.NewDatabase.TryGetSchemaNamed(oldSchema.Name)
			var newTable *model.Table
			if newSchema != nil {
				GlobalOperations.SetContextReplicaSetId(newSchema.SlonySetId)
				newTable = newSchema.TryGetTableNamed(oldTable.Name)
			}

			// NOTE: when dropping constraints, GlobalDBX.RenamedTableCheckPointer() is not called for oldTable
			// as GlobalDiffConstraints.DiffConstraintsTable() will do rename checking when recreating constraints for renamed tables
			GlobalDiffConstraints.DropConstraintsTable(stage1, oldSchema, oldTable, newSchema, newTable, ConstraintTypeConstraint)
			GlobalDiffConstraints.DropConstraintsTable(stage1, oldSchema, oldTable, newSchema, newTable, ConstraintTypePrimaryKey)
		}

		processedSchemas = map[string]bool{}
		for _, newEntry := range self.NewTableDependency {
			newSchema := newEntry.Schema
			if newSchema != nil {
				GlobalOperations.SetContextReplicaSetId(newSchema.SlonySetId)
			}
			oldSchema := dbsteward.OldDatabase.TryGetSchemaNamed(newSchema.Name)

			// schema level stuff should only be done once, keep track of which ones we have done
			// see above for pre table creation stuff
			// see below for post table creation stuff
			if !processedSchemas[newSchema.Name] {
				GlobalDiffSequences.DiffSequences(stage1, oldSchema, newSchema)
				processedSchemas[newSchema.Name] = true
			}

			newTable := newEntry.Table
			var oldTable *model.Table
			if oldSchema != nil {
				oldTable = oldSchema.TryGetTableNamed(newTable.Name)
			}

			// if they are defined in the old definition,
			// oldSchema and oldTable are already established pointers
			// when a table has an oldTableName oldSchemaName specified,
			// GlobalDBX.RenamedTableCheckPointer() will modify these pointers to be the old table
			oldSchema, oldTable = lib.GlobalDBX.RenamedTableCheckPointer(oldSchema, oldTable, newSchema, newTable)
			err := GlobalDiffTables.CreateTable(stage1, oldSchema, newSchema, newTable)
			lib.GlobalDBSteward.FatalIfError(err, "while creating table %s.%s", newSchema.Name, newTable.Name)
			err = GlobalDiffTables.DiffTable(stage1, stage3, oldSchema, oldTable, newSchema, newTable)
			lib.GlobalDBSteward.FatalIfError(err, "while diffing table %s.%s", newSchema.Name, newTable.Name)
			GlobalDiffIndexes.DiffIndexesTable(stage1, oldSchema, oldTable, newSchema, newTable)
			GlobalDiffTables.DiffClustersTable(stage1, oldSchema, oldTable, newSchema, newTable)
			GlobalDiffConstraints.CreateConstraintsTable(stage1, oldSchema, oldTable, newSchema, newTable, ConstraintTypePrimaryKey)
			GlobalDiffTriggers.DiffTriggersTable(stage1, oldSchema, oldTable, newSchema, newTable)

			// HACK: For now, we'll generate foreign key constraints in stage 4 in updateData below
			// https://github.com/dbsteward/dbsteward/issues/142
			GlobalDiffConstraints.CreateConstraintsTable(stage1, oldSchema, oldTable, newSchema, newTable, ConstraintTypeConstraint&^ConstraintTypeForeign)
		}

		// drop old tables in reverse dependency order
		for i := len(self.OldTableDependency) - 1; i >= 0; i -= 1 {
			oldEntry := self.OldTableDependency[i]
			oldSchema := oldEntry.Schema
			oldTable := oldEntry.Table

			newSchema := dbsteward.NewDatabase.TryGetSchemaNamed(oldSchema.Name)
			GlobalDiffTables.DropTable(stage3, oldSchema, oldTable, newSchema)
		}
	}

	GlobalDiffViews.CreateViewsOrdered(stage3, dbsteward.OldDatabase, dbsteward.NewDatabase)
}

func (self *Diff) updatePermissions(stage1 output.OutputFileSegmenter, stage3 output.OutputFileSegmenter) {
	// TODO(feat) what if readonly user changed? we need to rebuild those grants
	// TODO(feat) what about removed permissions, shouldn't we REVOKE those?

	newDoc := lib.GlobalDBSteward.NewDatabase
	oldDoc := lib.GlobalDBSteward.OldDatabase
	for _, newSchema := range newDoc.Schemas {
		GlobalOperations.SetContextReplicaSetId(newSchema.SlonySetId)
		oldSchema := oldDoc.TryGetSchemaNamed(newSchema.Name)

		for _, newGrant := range newSchema.Grants {
			if oldSchema == nil || !model.HasPermissionsOf(oldSchema, newGrant, model.SqlFormatPgsql8) {
				stage1.WriteSql(GlobalSchema.GetGrantSql(newDoc, newSchema, newGrant)...)
			}
		}

		for _, newTable := range newSchema.Tables {
			GlobalOperations.SetContextReplicaSetId(newTable.SlonySetId)
			oldTable := oldSchema.TryGetTableNamed(newTable.Name)
			isRenamed, err := GlobalDiffTables.IsRenamedTable(newSchema, newTable)
			lib.GlobalDBSteward.FatalIfError(err, "while updating permissions")
			if isRenamed {
				// skip permission diffing on it, it is the same
				// TODO(feat) that seems unlikely? we should probably check permissions on renamed table
				continue
			}
			for _, newGrant := range newTable.Grants {
				if oldTable == nil || !model.HasPermissionsOf(oldTable, newGrant, model.SqlFormatPgsql8) {
					stage1.WriteSql(GlobalTable.GetGrantSql(newDoc, newSchema, newTable, newGrant)...)
				}
			}
		}

		for _, newSeq := range newSchema.Sequences {
			oldSeq := oldSchema.TryGetSequenceNamed(newSeq.Name)
			for _, newGrant := range newSeq.Grants {
				if oldSeq == nil || !model.HasPermissionsOf(oldSeq, newGrant, model.SqlFormatPgsql8) {
					stage1.WriteSql(GlobalSequence.GetGrantSql(newDoc, newSchema, newSeq, newGrant)...)
				}
			}
		}

		for _, newFunc := range newSchema.Functions {
			oldFunc := oldSchema.TryGetFunctionMatching(newFunc)
			for _, newGrant := range newFunc.Grants {
				if oldFunc == nil || !model.HasPermissionsOf(oldFunc, newGrant, model.SqlFormatPgsql8) {
					stage1.WriteSql(GlobalFunction.GetGrantSql(newDoc, newSchema, newFunc, newGrant)...)
				}
			}
		}

		for _, newView := range newSchema.Views {
			oldView := oldSchema.TryGetViewNamed(newView.Name)
			for _, newGrant := range newView.Grants {
				if lib.GlobalDBSteward.AlwaysRecreateViews || oldView == nil || !model.HasPermissionsOf(oldView, newGrant, model.SqlFormatPgsql8) || !oldView.Equals(newView, model.SqlFormatPgsql8) {
					stage3.WriteSql(GlobalView.GetGrantSql(newDoc, newSchema, newView, newGrant)...)
				}
			}
		}
	}
}
func (self *Diff) updateData(ofs output.OutputFileSegmenter, deleteMode bool) {
	if len(self.NewTableDependency) > 0 {
		for i := 0; i < len(self.NewTableDependency); i += 1 {
			item := self.NewTableDependency[i]
			// go in reverse when in delete mode
			if deleteMode {
				item = self.NewTableDependency[len(self.NewTableDependency)-1-i]
			}

			newSchema := item.Schema
			newTable := item.Table
			oldSchema := lib.GlobalDBSteward.OldDatabase.TryGetSchemaNamed(newSchema.Name)
			oldTable := oldSchema.TryGetTableNamed(newTable.Name)
			GlobalOperations.SetContextReplicaSetId(newSchema.SlonySetId)

			isRenamed, err := GlobalDiffTables.IsRenamedTable(newSchema, newTable)
			lib.GlobalDBSteward.FatalIfError(err, "while updating data")
			if isRenamed {
				lib.GlobalDBSteward.Info("%s.%s used to be called %s - will diff data against that definition", newSchema.Name, newTable.Name, newTable.OldTableName)
				oldSchema = GlobalTable.GetOldTableSchema(newSchema, newTable)
				oldTable = GlobalTable.GetOldTable(newSchema, newTable)
			}

			if deleteMode {
				// TODO(go,3) clean up inconsistencies between e.g. GetDeleteDataSql and DiffData wrt writing sql to an ofs
				// TODO(feat) aren't deletes supposed to go in stage 2?
				ofs.WriteSql(GlobalDiffTables.GetDeleteDataSql(oldSchema, oldTable, newSchema, newTable)...)
			} else {
				ofs.WriteSql(GlobalDiffTables.GetCreateDataSql(oldSchema, oldTable, newSchema, newTable)...)

				// HACK: For now, we'll generate foreign key constraints in stage 4 after inserting data
				// https://github.com/dbsteward/dbsteward/issues/142
				GlobalDiffConstraints.CreateConstraintsTable(ofs, oldSchema, oldTable, newSchema, newTable, ConstraintTypeForeign)
			}
		}
	} else {
		// dependency order unknown, hit them in natural order
		// TODO(feat) the above switches on deleteMode, this does not. we never delete data if table dep order is unknown?
		for _, newSchema := range lib.GlobalDBSteward.NewDatabase.Schemas {
			GlobalOperations.SetContextReplicaSetId(newSchema.SlonySetId)
			oldSchema := lib.GlobalDBSteward.OldDatabase.TryGetSchemaNamed(newSchema.Name)
			GlobalDiffTables.DiffData(ofs, oldSchema, newSchema)
		}
	}
}
