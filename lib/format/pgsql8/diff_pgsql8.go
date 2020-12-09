package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
)

var GlobalDiff *Diff = NewDiff()

type Diff struct {
	OldTableDependency []*lib.TableDepEntry
	NewTableDependency []*lib.TableDepEntry
}

func NewDiff() *Diff {
	return &Diff{}
}

func (self *Diff) UpdateDatabaseConfigParameters(ofs lib.OutputFileSegmenter, oldDoc *model.Definition, newDoc *model.Definition) {
	// TODO(go,pgsql)
}

func (self *Diff) DiffDoc(oldFile, newFile string, oldDoc, newDoc *model.Definition, upgradePrefix string) {
	// TODO(go,pgsql)
}
