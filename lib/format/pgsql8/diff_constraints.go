package pgsql8

import (
	"fmt"
	"log/slog"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/sql99"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

func createConstraints(dbs *lib.DBSteward, ofs output.OutputFileSegmenter, oldSchema, newSchema *ir.Schema, constraintType sql99.ConstraintType) {
	for _, newTable := range newSchema.Tables {
		var oldTable *ir.Table
		if oldSchema != nil {
			// TODO(feat) what about renames?
			oldTable = oldSchema.TryGetTableNamed(newTable.Name)
		}
		createConstraintsTable(dbs, ofs, oldSchema, oldTable, newSchema, newTable, constraintType)
	}
}

func createConstraintsTable(dbs *lib.DBSteward, ofs output.OutputFileSegmenter, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table, constraintType sql99.ConstraintType) error {
	isRenamed, err := dbs.OldDatabase.IsRenamedTable(slog.Default(), newSchema, newTable)
	if err != nil {
		return fmt.Errorf("while checking if table was renamed: %w", err)
	}
	if isRenamed {
		// remove all constraints and recreate with new table name conventions
		constraints, err := getTableConstraints(dbs.OldDatabase, oldSchema, oldTable, constraintType)
		if err != nil {
			return err
		}
		for _, constraint := range constraints {
			// rewrite the constraint definer to refer to the new table
			// so the constraint by the old, but part of the new table
			// will be referenced properly in the drop statement
			constraint.Schema = newSchema
			constraint.Table = newTable
			ofs.WriteSql(getTableConstraintDropSql(constraint)...)
		}

		// add all still-defined constraints back and any new ones to the table
		constraints, err = getTableConstraints(dbs.NewDatabase, newSchema, newTable, constraintType)
		if err != nil {
			return err
		}
		for _, constraint := range constraints {
			ofs.WriteSql(getTableContraintCreationSql(constraint)...)
		}

		return nil
	}
	constraints, err := getNewConstraints(dbs, oldSchema, oldTable, newSchema, newTable, constraintType)
	if err != nil {
		return err
	}
	for _, constraint := range constraints {
		ofs.WriteSql(getTableContraintCreationSql(constraint)...)
	}
	return nil
}

func dropConstraints(dbs *lib.DBSteward, ofs output.OutputFileSegmenter, oldSchema, newSchema *ir.Schema, constraintType sql99.ConstraintType) error {
	for _, newTable := range newSchema.Tables {
		var oldTable *ir.Table
		if oldSchema != nil {
			// TODO(feat) what about renames?
			oldTable = oldSchema.TryGetTableNamed(newTable.Name)
		}
		err := dropConstraintsTable(dbs, ofs, oldSchema, oldTable, newSchema, newTable, constraintType)
		if err != nil {
			return err
		}
	}
	return nil
}

func dropConstraintsTable(dbs *lib.DBSteward, ofs output.OutputFileSegmenter, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table, constraintType sql99.ConstraintType) error {
	constraints, err := getOldConstraints(dbs, oldSchema, oldTable, newSchema, newTable, constraintType)
	if err != nil {
		return err
	}
	for _, constraint := range constraints {
		ofs.WriteSql(getTableConstraintDropSql(constraint)...)
	}
	return nil
}

func getOldConstraints(dbs *lib.DBSteward, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table, constraintType sql99.ConstraintType) ([]*sql99.TableConstraint, error) {
	out := []*sql99.TableConstraint{}
	if newTable != nil && oldTable != nil {
		oldDb := dbs.OldDatabase
		newDb := dbs.NewDatabase
		constraints, err := getTableConstraints(oldDb, oldSchema, oldTable, constraintType)
		if err != nil {
			return nil, err
		}
		for _, oldConstraint := range constraints {
			newConstraint, err := tryGetTableConstraintNamed(newDb, newSchema, newTable, oldConstraint.Name, constraintType)
			if err != nil {
				return nil, err
			}
			if newConstraint == nil {
				out = append(out, oldConstraint)
				continue
			}
			oldConstraintWithRenamedTable, err := constraintDependsOnRenamedTable(dbs, newDb, oldConstraint)
			if err != nil {
				return nil, err
			}
			newConstraintWithRenamedTable, err := constraintDependsOnRenamedTable(dbs, newDb, newConstraint)
			if err != nil {
				return nil, err
			}
			if !newConstraint.Equals(oldConstraint) || oldConstraintWithRenamedTable || newConstraintWithRenamedTable {
				out = append(out, oldConstraint)
			}
		}
	}
	return out, nil
}

func getNewConstraints(dbs *lib.DBSteward, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table, constraintType sql99.ConstraintType) ([]*sql99.TableConstraint, error) {
	out := []*sql99.TableConstraint{}
	if newTable != nil {
		oldDb := dbs.OldDatabase
		newDb := dbs.NewDatabase
		newConstraints, err := getTableConstraints(newDb, newSchema, newTable, constraintType)
		if err != nil {
			return nil, err
		}
		for _, newConstraint := range newConstraints {
			oldConstraint, err := tryGetTableConstraintNamed(oldDb, oldSchema, oldTable, newConstraint.Name, constraintType)
			if err != nil {
				return nil, err
			}
			renamedTable, err := constraintDependsOnRenamedTable(dbs, newDb, newConstraint)
			if err != nil {
				return nil, err
			}
			if oldConstraint == nil || !oldConstraint.Equals(newConstraint) || renamedTable {
				out = append(out, newConstraint)
			}
		}
	}
	return out, nil
}
