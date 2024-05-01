package pgsql8

import (
	"log/slog"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/sql99"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

func createConstraints(ofs output.OutputFileSegmenter, oldSchema, newSchema *ir.Schema, constraintType sql99.ConstraintType) {
	for _, newTable := range newSchema.Tables {
		var oldTable *ir.Table
		if oldSchema != nil {
			// TODO(feat) what about renames?
			oldTable = oldSchema.TryGetTableNamed(newTable.Name)
		}
		createConstraintsTable(ofs, oldSchema, oldTable, newSchema, newTable, constraintType)
	}
}

func createConstraintsTable(ofs output.OutputFileSegmenter, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table, constraintType sql99.ConstraintType) {
	isRenamed, err := lib.GlobalDBSteward.OldDatabase.IsRenamedTable(slog.Default(), newSchema, newTable)
	lib.GlobalDBSteward.FatalIfError(err, "while checking if table was renamed")
	if isRenamed {
		// remove all constraints and recreate with new table name conventions
		for _, constraint := range getTableConstraints(lib.GlobalDBSteward.OldDatabase, oldSchema, oldTable, constraintType) {
			// rewrite the constraint definer to refer to the new table
			// so the constraint by the old, but part of the new table
			// will be referenced properly in the drop statement
			constraint.Schema = newSchema
			constraint.Table = newTable
			ofs.WriteSql(getTableConstraintDropSql(constraint)...)
		}

		// add all still-defined constraints back and any new ones to the table
		for _, constraint := range getTableConstraints(lib.GlobalDBSteward.NewDatabase, newSchema, newTable, constraintType) {
			ofs.WriteSql(getTableContraintCreationSql(constraint)...)
		}

		return
	}

	for _, constraint := range getNewConstraints(oldSchema, oldTable, newSchema, newTable, constraintType) {
		ofs.WriteSql(getTableContraintCreationSql(constraint)...)
	}
}

func dropConstraints(ofs output.OutputFileSegmenter, oldSchema, newSchema *ir.Schema, constraintType sql99.ConstraintType) {
	for _, newTable := range newSchema.Tables {
		var oldTable *ir.Table
		if oldSchema != nil {
			// TODO(feat) what about renames?
			oldTable = oldSchema.TryGetTableNamed(newTable.Name)
		}
		dropConstraintsTable(ofs, oldSchema, oldTable, newSchema, newTable, constraintType)
	}
}

func dropConstraintsTable(ofs output.OutputFileSegmenter, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table, constraintType sql99.ConstraintType) {
	for _, constraint := range getOldConstraints(oldSchema, oldTable, newSchema, newTable, constraintType) {
		ofs.WriteSql(getTableConstraintDropSql(constraint)...)
	}
}

func getOldConstraints(oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table, constraintType sql99.ConstraintType) []*sql99.TableConstraint {
	out := []*sql99.TableConstraint{}
	if newTable != nil && oldTable != nil {
		oldDb := lib.GlobalDBSteward.OldDatabase
		newDb := lib.GlobalDBSteward.NewDatabase
		for _, oldConstraint := range getTableConstraints(oldDb, oldSchema, oldTable, constraintType) {
			newConstraint := tryGetTableConstraintNamed(newDb, newSchema, newTable, oldConstraint.Name, constraintType)
			if newConstraint == nil || !newConstraint.Equals(oldConstraint) || constraintDependsOnRenamedTable(newDb, oldConstraint) || constraintDependsOnRenamedTable(newDb, newConstraint) {
				out = append(out, oldConstraint)
			}
		}
	}
	return out
}

func getNewConstraints(oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table, constraintType sql99.ConstraintType) []*sql99.TableConstraint {
	out := []*sql99.TableConstraint{}
	if newTable != nil {
		oldDb := lib.GlobalDBSteward.OldDatabase
		newDb := lib.GlobalDBSteward.NewDatabase
		for _, newConstraint := range getTableConstraints(newDb, newSchema, newTable, constraintType) {
			oldConstraint := tryGetTableConstraintNamed(oldDb, oldSchema, oldTable, newConstraint.Name, constraintType)
			if oldConstraint == nil || !oldConstraint.Equals(newConstraint) || constraintDependsOnRenamedTable(newDb, newConstraint) {
				out = append(out, newConstraint)
			}
		}
	}
	return out
}
