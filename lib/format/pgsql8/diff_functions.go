package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

func diffFunctions(dbs *lib.DBSteward, stage1 output.OutputFileSegmenter, stage3 output.OutputFileSegmenter, oldSchema *ir.Schema, newSchema *ir.Schema) error {
	// drop functions that no longer exist in stage 3
	if oldSchema != nil {
		for _, oldFunction := range oldSchema.Functions {
			if newSchema.TryGetFunctionMatching(oldFunction) == nil {
				stage3.WriteSql(getFunctionDropSql(oldSchema, oldFunction)...)
			}
		}
	}

	// add new functions and replace modified functions
	for _, newFunction := range newSchema.Functions {
		oldFunction := oldSchema.TryGetFunctionMatching(newFunction)
		if oldFunction == nil || !oldFunction.Equals(newFunction, ir.SqlFormatPgsql8) {
			create, err := getFunctionCreationSql(dbs, newSchema, newFunction)
			if err != nil {
				return nil
			}
			stage1.WriteSql(create...)
		} else if newFunction.ForceRedefine {
			stage1.WriteSql(sql.NewComment("Function %s.%s has forceRedefine set to true", newSchema.Name, newFunction.Name))
			create, err := getFunctionCreationSql(dbs, newSchema, newFunction)
			if err != nil {
				return nil
			}
			stage1.WriteSql(create...)
		} else {
			oldReturnType := oldSchema.TryGetTypeNamed(newFunction.Returns)
			newReturnType := newSchema.TryGetTypeNamed(newFunction.Returns)
			if oldReturnType != nil && newReturnType != nil && !oldReturnType.Equals(newReturnType) {
				stage1.WriteSql(sql.NewComment("Function %s.%s return type %s has changed", newSchema.Name, newFunction.Name, newReturnType.Name))
				create, err := getFunctionCreationSql(dbs, newSchema, newFunction)
				if err != nil {
					return nil
				}
				stage1.WriteSql(create...)
			}
		}
	}
	return nil
}
