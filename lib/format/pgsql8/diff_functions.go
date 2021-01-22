package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffFunctions struct {
}

func NewDiffFunctions() *DiffFunctions {
	return &DiffFunctions{}
}

func (self *DiffFunctions) DiffFunctions(stage1 output.OutputFileSegmenter, stage3 output.OutputFileSegmenter, oldSchema *model.Schema, newSchema *model.Schema) {
	// drop functions that no longer exist in stage 3
	if oldSchema != nil {
		for _, oldFunction := range oldSchema.Functions {
			if newSchema.TryGetFunctionMatching(oldFunction) == nil {
				stage3.WriteSql(GlobalFunction.GetDropSql(oldSchema, oldFunction)...)
			}
		}
	}

	// add new functions and replace modified functions
	for _, newFunction := range newSchema.Functions {
		oldFunction := oldSchema.TryGetFunctionMatching(newFunction)
		if oldFunction == nil || !oldFunction.Equals(newFunction, model.SqlFormatPgsql8) {
			stage1.WriteSql(GlobalFunction.GetCreationSql(newSchema, newFunction)...)
		} else if newFunction.ForceRedefine {
			stage1.WriteSql(sql.NewComment("Function %s.%s has forceRedefine set to true", newSchema.Name, newFunction.Name))
			stage1.WriteSql(GlobalFunction.GetCreationSql(newSchema, newFunction)...)
		} else {
			oldReturnType := oldSchema.TryGetTypeNamed(newFunction.Returns)
			newReturnType := newSchema.TryGetTypeNamed(newFunction.Returns)
			if oldReturnType != nil && newReturnType != nil && !oldReturnType.Equals(newReturnType) {
				stage1.WriteSql(sql.NewComment("Function %s.%s return type %s has changed", newSchema.Name, newFunction.Name, newReturnType.Name))
				stage1.WriteSql(GlobalFunction.GetCreationSql(newSchema, newFunction)...)
			}
		}
	}
}
