package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
)

var GlobalDiff *Diff = NewDiff()

type Diff struct {
}

func NewDiff() *Diff {
	return &Diff{}
}

func (self *Diff) UpdateDatabaseConfigParameters(ofs lib.OutputFileSegmenter, oldDoc *model.Definition, newDoc *model.Definition) {
	// TODO(go,pgsql)
}
