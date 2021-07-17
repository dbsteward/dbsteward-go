package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffTypes struct {
}

func NewDiffTypes() *DiffTypes {
	return &DiffTypes{}
}

func (self *DiffTypes) DiffTypes(ofs output.OutputFileSegmenter, oldSchema *model.Schema, newSchema *model.Schema) {
	// TODO(go,mysql) implement me; see mysql5_diff_types::apply_changes
}
