package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffViews struct {
}

func NewDiffViews() *DiffViews {
	return &DiffViews{}
}

func (self *DiffViews) CreateViewsOrdered(ofs output.OutputFileSegmenter, oldDoc *model.Definition, newDoc *model.Definition) {
	// TODO(go,mysql) implement me
}
