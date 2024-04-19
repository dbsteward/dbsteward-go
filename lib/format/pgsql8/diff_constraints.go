package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/sql99"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffConstraints struct {
}

func NewDiffConstraints() *DiffConstraints {
	return &DiffConstraints{}
}

func (self *DiffConstraints) CreateConstraints(ofs output.OutputFileSegmenter, oldSchema, newSchema *ir.Schema, constraintType sql99.ConstraintType) {
	for _, newTable := range newSchema.Tables {
		var oldTable *ir.Table
		if oldSchema != nil {
			// TODO(feat) what about renames?
			oldTable = oldSchema.TryGetTableNamed(newTable.Name)
		}
		self.CreateConstraintsTable(ofs, oldSchema, oldTable, newSchema, newTable, constraintType)
	}
}

func (self *DiffConstraints) CreateConstraintsTable(ofs output.OutputFileSegmenter, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table, constraintType sql99.ConstraintType) {
	isRenamed, err := lib.GlobalDBX.IsRenamedTable(newSchema, newTable)
	lib.GlobalDBSteward.FatalIfError(err, "while checking if table was renamed")
	if isRenamed {
		// remove all constraints and recreate with new table name conventions
		for _, constraint := range GlobalConstraint.GetTableConstraints(lib.GlobalDBSteward.OldDatabase, oldSchema, oldTable, constraintType) {
			// rewrite the constraint definer to refer to the new table
			// so the constraint by the old, but part of the new table
			// will be referenced properly in the drop statement
			constraint.Schema = newSchema
			constraint.Table = newTable
			ofs.WriteSql(GlobalConstraint.GetDropSql(constraint)...)
		}

		// add all still-defined constraints back and any new ones to the table
		for _, constraint := range GlobalConstraint.GetTableConstraints(lib.GlobalDBSteward.NewDatabase, newSchema, newTable, constraintType) {
			ofs.WriteSql(GlobalConstraint.GetCreationSql(constraint)...)
		}

		return
	}

	for _, constraint := range self.GetNewConstraints(oldSchema, oldTable, newSchema, newTable, constraintType) {
		ofs.WriteSql(GlobalConstraint.GetCreationSql(constraint)...)
	}
}

func (self *DiffConstraints) DropConstraints(ofs output.OutputFileSegmenter, oldSchema, newSchema *ir.Schema, constraintType sql99.ConstraintType) {
	for _, newTable := range newSchema.Tables {
		var oldTable *ir.Table
		if oldSchema != nil {
			// TODO(feat) what about renames?
			oldTable = oldSchema.TryGetTableNamed(newTable.Name)
		}
		self.DropConstraintsTable(ofs, oldSchema, oldTable, newSchema, newTable, constraintType)
	}
}

func (self *DiffConstraints) DropConstraintsTable(ofs output.OutputFileSegmenter, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table, constraintType sql99.ConstraintType) {
	for _, constraint := range self.GetOldConstraints(oldSchema, oldTable, newSchema, newTable, constraintType) {
		ofs.WriteSql(GlobalConstraint.GetDropSql(constraint)...)
	}
}

func (self *DiffConstraints) GetOldConstraints(oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table, constraintType sql99.ConstraintType) []*sql99.TableConstraint {
	out := []*sql99.TableConstraint{}
	if newTable != nil && oldTable != nil {
		oldDb := lib.GlobalDBSteward.OldDatabase
		newDb := lib.GlobalDBSteward.NewDatabase
		for _, oldConstraint := range GlobalConstraint.GetTableConstraints(oldDb, oldSchema, oldTable, constraintType) {
			newConstraint := GlobalConstraint.TryGetTableConstraintNamed(newDb, newSchema, newTable, oldConstraint.Name, constraintType)
			if newConstraint == nil || !newConstraint.Equals(oldConstraint) || GlobalConstraint.DependsOnRenamedTable(newDb, oldConstraint) || GlobalConstraint.DependsOnRenamedTable(newDb, newConstraint) {
				out = append(out, oldConstraint)
			}
		}
	}
	return out
}

func (self *DiffConstraints) GetNewConstraints(oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table, constraintType sql99.ConstraintType) []*sql99.TableConstraint {
	out := []*sql99.TableConstraint{}
	if newTable != nil {
		oldDb := lib.GlobalDBSteward.OldDatabase
		newDb := lib.GlobalDBSteward.NewDatabase
		for _, newConstraint := range GlobalConstraint.GetTableConstraints(newDb, newSchema, newTable, constraintType) {
			oldConstraint := GlobalConstraint.TryGetTableConstraintNamed(oldDb, oldSchema, oldTable, newConstraint.Name, constraintType)
			if oldConstraint == nil || !oldConstraint.Equals(newConstraint) || GlobalConstraint.DependsOnRenamedTable(newDb, newConstraint) {
				out = append(out, newConstraint)
			}
		}
	}
	return out
}
