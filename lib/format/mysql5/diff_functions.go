package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffFunctions struct {
}

func NewDiffFunctions() *DiffFunctions {
	return &DiffFunctions{}
}

func (self *DiffFunctions) DiffFunctions(stage1, stage3 output.OutputFileSegmenter, oldSchema *model.Schema, newSchema *model.Schema) {
	// TODO(go,mysql) implement me; see mysql5_diff_functions::diff_functions
}
