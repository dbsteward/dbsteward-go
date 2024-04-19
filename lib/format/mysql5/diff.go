package mysql5

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/sql99"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Diff struct {
	*sql99.Diff
	OldTableDependency []*ir.TableRef
	NewTableDependency []*ir.TableRef
}

func NewDiff() *Diff {
	diff := &Diff{
		Diff: sql99.NewDiff(GlobalLookup),
	}
	diff.Diff.Diff = diff
	return diff
}

// DiffDoc implemented by sql99.Diff

func (self *Diff) DiffDocWork(stage1, stage2, stage3, stage4 output.OutputFileSegmenter) {
	dbsteward := lib.GlobalDBSteward
	dbx := lib.GlobalDBX

	// start with pre-upgrade sql statements that prepare the database to take on its changes
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage1, "STAGE1BEFORE")
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage2, "STAGE2BEFORE")

	// TODO(go,nth) document why mysql revokes and updates permissions but pgsql doesn't
	//              or: why does pgsql never revoke?

	dbsteward.Info("Revoke Permissions")
	self.revokePermissions(stage1)

	dbsteward.Info("Update Structure")
	self.updateStructure(stage1, stage3)

	dbsteward.Info("Update Permissions")
	self.updatePermissions(stage1, stage3)

	dbsteward.Info("Update Data")
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

	dbx.BuildStagedSql(dbsteward.NewDatabase, stage1, "STAGE1")
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage2, "STAGE2")
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage3, "STAGE3")
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage4, "STAGE4")
}

func (self *Diff) revokePermissions(stage1 output.OutputFileSegmenter) {
	// TODO(feat) do granular diffing instead of wholesale revoke+regrant
	// TODO(feat) what if objects were renamed
	oldDoc := lib.GlobalDBSteward.OldDatabase
	for _, oldSchema := range oldDoc.Schemas {
		for _, oldGrant := range oldSchema.Grants {
			stage1.WriteSql(GlobalSchema.GetRevokeSql(oldDoc, oldSchema, oldGrant)...)
		}
		for _, oldTable := range oldSchema.Tables {
			for _, oldGrant := range oldSchema.Grants {
				stage1.WriteSql(GlobalTable.GetRevokeSql(oldDoc, oldSchema, oldTable, oldGrant)...)
			}
		}
		stage1.WriteSql(GlobalSequence.GetMultiRevokeSql(oldDoc, oldSchema, oldSchema.Sequences)...)
		for _, oldFunc := range oldSchema.Functions {
			for _, oldGrant := range oldFunc.Grants {
				stage1.WriteSql(GlobalFunction.GetRevokeSql(oldDoc, oldSchema, oldFunc, oldGrant)...)
			}
		}
		for _, oldView := range oldSchema.Views {
			for _, oldGrant := range oldView.Grants {
				stage1.WriteSql(GlobalView.GetRevokeSql(oldDoc, oldSchema, oldView, oldGrant)...)
			}
		}
	}
}

func (self *Diff) updateStructure(stage1, stage3 output.OutputFileSegmenter) {
	dbsteward := lib.GlobalDBSteward
	oldDoc := dbsteward.OldDatabase
	newDoc := dbsteward.NewDatabase

	if GlobalOperations.UseSchemaNamePrefix {
		dbsteward.Info("Drop Old Schemas")
		self.DropOldSchemas(stage3)
	} else if len(dbsteward.NewDatabase.Schemas) > 1 || len(dbsteward.OldDatabase.Schemas) > 1 {
		dbsteward.Fatal("You cannot use more than one schema in mysql5 without schema name prefixing\nPass the --useschemaprefix flag to turn this on")
	}

	GlobalDiffViews.DropViewsOrdered(stage1, dbsteward.OldDatabase, dbsteward.NewDatabase)

	// TODO(feat) implement mysql5_language ? no relevant conversion exists see other TODO's stating this
	//mysql5_diff_languages::diff_languages($ofs1);

	// TODO(go,3) should we just always use table deps?
	if len(self.NewTableDependency) == 0 {
		for _, newSchema := range newDoc.Schemas {
			// TODO(feat) this does not honor old*Name attributes, does it matter?
			oldSchema := oldDoc.TryGetSchemaNamed(newSchema.Name)

			GlobalDiffTypes.DiffTypes(stage1, oldSchema, newSchema)
			GlobalDiffFunctions.DiffFunctions(stage1, stage3, oldSchema, newSchema)
			GlobalDiffSequences.DiffSequences(stage1, stage3, oldSchema, newSchema)
			// remove old constraints before table constraints, so the SQL statements succeed
			GlobalDiffConstraints.DropConstraints(stage1, oldSchema, newSchema, sql99.ConstraintTypeConstraint)
			GlobalDiffConstraints.DropConstraints(stage1, oldSchema, newSchema, sql99.ConstraintTypePrimaryKey)
			GlobalDiffTables.DropTables(stage3, oldSchema, newSchema)
			GlobalDiffTables.DiffTables(stage1, stage3, oldSchema, newSchema)
			// mysql5_diff_indexes::diff_indexes($ofs1, $old_schema, $new_schema)
			GlobalDiffConstraints.CreateConstraints(stage1, oldSchema, newSchema, sql99.ConstraintTypePrimaryKey)
			GlobalDiffTriggers.DiffTriggers(stage1, oldSchema, newSchema)
		}

		// non-primary key constraints may be inter-schema dependant, and dependant on other's primary keys
		// and therefore should be done after object creation sections
		for _, newSchema := range newDoc.Schemas {
			oldSchema := oldDoc.TryGetSchemaNamed(newSchema.Name)
			GlobalDiffConstraints.CreateConstraints(stage1, oldSchema, newSchema, sql99.ConstraintTypeConstraint)
		}
	} else {
		// use table dependency order to do structural changes in an intelligent order
		// make sure we only process each schema once
		processedSchemas := map[string]bool{}
		for _, newEntry := range self.NewTableDependency {
			newSchema := newEntry.Schema
			oldSchema := dbsteward.OldDatabase.TryGetSchemaNamed(newSchema.Name)

			if !processedSchemas[newSchema.Name] {
				GlobalDiffTypes.DiffTypes(stage1, oldSchema, newSchema)
				GlobalDiffFunctions.DiffFunctions(stage1, stage3, oldSchema, newSchema)
				processedSchemas[newSchema.Name] = true
			}
		}

		// remove all old constraints before new contraints, in reverse dependency order
		// TODO(go,mysql) REVERSE dependency order
		for _, oldEntry := range self.OldTableDependency {
			oldSchema := oldEntry.Schema
			oldTable := oldEntry.Table

			newSchema := dbsteward.NewDatabase.TryGetSchemaNamed(oldSchema.Name)
			var newTable *ir.Table
			if newSchema != nil {
				newTable = newSchema.TryGetTableNamed(oldTable.Name)
			}

			// NOTE: when dropping constraints, GlobalDBX.RenamedTableCheckPointer() is not called for oldTable
			// as GlobalDiffConstraints.DiffConstraintsTable() will do rename checking when recreating constraints for renamed tables
			GlobalDiffConstraints.DropConstraintsTable(stage1, oldSchema, oldTable, newSchema, newTable, sql99.ConstraintTypeConstraint)
			GlobalDiffConstraints.DropConstraintsTable(stage1, oldSchema, oldTable, newSchema, newTable, sql99.ConstraintTypePrimaryKey)
		}

		processedSchemas = map[string]bool{}
		for _, newEntry := range self.NewTableDependency {
			newSchema := newEntry.Schema
			oldSchema := dbsteward.OldDatabase.TryGetSchemaNamed(newSchema.Name)

			// schema level stuff should only be done once, keep track of which ones we have done
			// see above for pre table creation stuff
			// see below for post table creation stuff
			if !processedSchemas[newSchema.Name] {
				GlobalDiffSequences.DiffSequences(stage1, stage3, oldSchema, newSchema)
				processedSchemas[newSchema.Name] = true
			}

			newTable := newEntry.Table
			var oldTable *ir.Table
			if oldSchema != nil {
				oldTable = oldSchema.TryGetTableNamed(newTable.Name)
			}

			oldSchema, oldTable = lib.GlobalDBX.RenamedTableCheckPointer(oldSchema, oldTable, newSchema, newTable)
			err := GlobalDiffTables.CreateTable(stage1, oldSchema, newSchema, newTable)
			dbsteward.FatalIfError(err, "while creating table %s.%s", newSchema.Name, newTable.Name)
			err = GlobalDiffTables.DiffTable(stage1, stage3, oldSchema, oldTable, newSchema, newTable)
			dbsteward.FatalIfError(err, "while diffing table %s.%s", newSchema.Name, newTable.Name)
			// mysql5_diff_indexes::diff_indexes_table($ofs1, $old_schema, $old_table, $new_schema, $new_table);
			GlobalDiffConstraints.CreateConstraintsTable(stage1, oldSchema, oldTable, newSchema, newTable, sql99.ConstraintTypePrimaryKey)
			GlobalDiffTriggers.DiffTriggersTable(stage1, oldSchema, oldTable, newSchema, newTable)

			// HACK: For now, we'll generate foreign key constraints in stage 4 in updateData below
			// https://github.com/dbsteward/dbsteward/issues/142
			GlobalDiffConstraints.CreateConstraintsTable(stage1, oldSchema, oldTable, newSchema, newTable, sql99.ConstraintTypeConstraint&^sql99.ConstraintTypeForeign)
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

func (self *Diff) updatePermissions(stage1, stage3 output.OutputFileSegmenter) {
	// TODO(feat) do granular diffing instead of wholesale revoke+regrant
	// TODO(feat) what if objects were renamed
	newDoc := lib.GlobalDBSteward.NewDatabase
	for _, newSchema := range newDoc.Schemas {
		for _, newGrant := range newSchema.Grants {
			stage1.WriteSql(GlobalSchema.GetGrantSql(newDoc, newSchema, newGrant)...)
		}
		for _, newTable := range newSchema.Tables {
			for _, newGrant := range newSchema.Grants {
				stage1.WriteSql(GlobalTable.GetGrantSql(newDoc, newSchema, newTable, newGrant)...)
			}
		}
		stage1.WriteSql(GlobalSequence.GetMultiGrantSql(newDoc, newSchema, newSchema.Sequences)...)
		for _, newFunc := range newSchema.Functions {
			for _, newGrant := range newFunc.Grants {
				stage1.WriteSql(GlobalFunction.GetGrantSql(newDoc, newSchema, newFunc, newGrant)...)
			}
		}
		for _, newView := range newSchema.Views {
			for _, newGrant := range newView.Grants {
				stage1.WriteSql(GlobalView.GetGrantSql(newDoc, newSchema, newView, newGrant)...)
			}
		}
	}
}

func (self *Diff) updateData(ofs output.OutputFileSegmenter, deleteMode bool) {
	// TODO(go,3) unify with pgsql
	if len(self.NewTableDependency) > 0 {
		for i := 0; i < len(self.NewTableDependency); i++ {
			item := self.NewTableDependency[i]
			// go in reverse when in delete mode
			if deleteMode {
				item = self.NewTableDependency[len(self.NewTableDependency)-1-i]
			}

			newSchema := item.Schema
			newTable := item.Table
			oldSchema := lib.GlobalDBSteward.OldDatabase.TryGetSchemaNamed(newSchema.Name)
			oldTable := oldSchema.TryGetTableNamed(newTable.Name)

			// TODO(feat) pgsq8 does a rename check here, should we also do that here?
			if deleteMode {
				// TODO(go,3) clean up inconsistencies between e.g. GetDeleteDataSql and DiffData wrt writing sql to an ofs
				// TODO(feat) aren't deletes supposed to go in stage 2?
				ofs.WriteSql(GlobalDiffTables.GetDeleteDataSql(oldSchema, oldTable, newSchema, newTable)...)
			} else {
				ofs.WriteSql(GlobalDiffTables.GetCreateDataSql(oldSchema, oldTable, newSchema, newTable)...)

				// HACK: For now, we'll generate foreign key constraints in stage 4 after inserting data
				// https://github.com/dbsteward/dbsteward/issues/142
				GlobalDiffConstraints.CreateConstraintsTable(ofs, oldSchema, oldTable, newSchema, newTable, sql99.ConstraintTypeForeign)
			}
		}
	} else {
		for _, newSchema := range lib.GlobalDBSteward.NewDatabase.Schemas {
			oldSchema := lib.GlobalDBSteward.OldDatabase.TryGetSchemaNamed(newSchema.Name)
			GlobalDiffTables.DiffData(ofs, oldSchema, newSchema)
		}
	}
}
