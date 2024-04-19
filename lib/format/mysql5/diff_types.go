package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffTypes struct {
}

func NewDiffTypes() *DiffTypes {
	return &DiffTypes{}
}

func (self *DiffTypes) DiffTypes(ofs output.OutputFileSegmenter, oldSchema *ir.Schema, newSchema *ir.Schema) {
	// TODO(go,mysql) implement me; see mysql5_diff_types::apply_changes
}
