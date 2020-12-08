package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
)

var GlobalDiffIndexes *DiffIndexes = NewDiffIndexes()

type DiffIndexes struct {
}

func NewDiffIndexes() *DiffIndexes {
	return &DiffIndexes{}
}

func (self *DiffIndexes) DiffIndexesTable(ofs lib.OutputFileSegmenter, oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) {
	// TODO(go,pgsql)
}
