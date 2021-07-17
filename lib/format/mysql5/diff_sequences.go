package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffSequences struct {
}

func NewDiffSequences() *DiffSequences {
	return &DiffSequences{}
}

func (self *DiffSequences) DiffSequences(stage1, stage3 output.OutputFileSegmenter, oldSchema *model.Schema, newSchema *model.Schema) {
	// TODO(go,mysql) implement me; see mysql5_diff_sequences::diff_sequences
}
