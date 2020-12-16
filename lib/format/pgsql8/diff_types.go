package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

var GlobalDiffTypes *DiffTypes = NewDiffTypes()

type DiffTypes struct {
}

func NewDiffTypes() *DiffTypes {
	return &DiffTypes{}
}

func (self *DiffTypes) ApplyChanges(ofs output.OutputFileSegmenter, oldSchema *model.Schema, newSchema *model.Schema) {
	// TODO(go,pgsql)
}
