package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

var GlobalDiffIndexes *DiffIndexes = NewDiffIndexes()

type DiffIndexes struct {
}

func NewDiffIndexes() *DiffIndexes {
	return &DiffIndexes{}
}

func (self *DiffIndexes) DiffIndexesTable(ofs output.OutputFileSegmenter, oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) {
	// TODO(go,pgsql)
}
