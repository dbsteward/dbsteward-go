package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffTriggers struct {
}

func NewDiffTriggers() *DiffTriggers {
	return &DiffTriggers{}
}

func (self *DiffTriggers) DiffTriggers(ofs output.OutputFileSegmenter, oldSchema *ir.Schema, newSchema *ir.Schema) {
	// TODO(go,mysql) implement me; see mysql5_diff_triggers::diff_triggers
}

func (self *DiffTriggers) DiffTriggersTable(ofs output.OutputFileSegmenter, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) {
	// TODO(go,mysql) implement me; see mysql5_diff_triggers::diff_triggers_table
}
