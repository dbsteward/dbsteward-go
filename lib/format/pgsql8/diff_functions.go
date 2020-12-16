package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

var GlobalDiffFunctions *DiffFunctions = NewDiffFunctions()

type DiffFunctions struct {
}

func NewDiffFunctions() *DiffFunctions {
	return &DiffFunctions{}
}

func (self *DiffFunctions) DiffFunctions(stage1 output.OutputFileSegmenter, stage3 output.OutputFileSegmenter, oldSchema *model.Schema, newSchema *model.Schema) {
	// TODO(go,pgsql)
}
