package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/format/sql99"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Diff struct {
	*sql99.Diff
	OldTableDependency []*model.TableRef
	NewTableDependency []*model.TableRef
}

func NewDiff() *Diff {
	diff := &Diff{
		Diff: sql99.NewDiff(GlobalLookup),
	}
	diff.Diff.Diff = diff
	return diff
}

// DiffDoc implemented by sql99.Diff

func (self *Diff) DiffDocWork(stage1, stage2, stage3, stage4 output.OutputFileSegmenter) {
	// TODO(go,mysql) implement me; see mysql5_diff::diff_doc_work
}
