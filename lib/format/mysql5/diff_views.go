package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffViews struct {
}

func NewDiffViews() *DiffViews {
	return &DiffViews{}
}

func (self *DiffViews) DropViewsOrdered(ofs output.OutputFileSegmenter, oldDoc, newDoc *ir.Definition) {
	// TODO(go,mysql) implement me
}

func (self *DiffViews) CreateViewsOrdered(ofs output.OutputFileSegmenter, oldDoc, newDoc *ir.Definition) {
	// TODO(go,mysql) implement me
}
