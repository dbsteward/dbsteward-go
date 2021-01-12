package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffTriggers struct {
}

func NewDiffTriggers() *DiffTriggers {
	return &DiffTriggers{}
}

func (self *DiffTriggers) DiffTriggers(ofs output.OutputFileSegmenter, oldSchema *model.Schema, newSchema *model.Schema) {
	// TODO(go,pgsql)
}

func (self *DiffTriggers) DiffTriggersTable(ofs output.OutputFileSegmenter, oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) {
	// TODO(go,pgsql)
}
