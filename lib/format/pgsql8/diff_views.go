package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

var GlobalDiffViews *DiffViews = NewDiffViews()

type DiffViews struct {
}

func NewDiffViews() *DiffViews {
	return &DiffViews{}
}

func (self *DiffViews) CreateViewsOrdered(ofs output.OutputFileSegmenter, oldDoc *model.Definition, newDoc *model.Definition) {
	// TODO(go,pgsql)
}

func (self *DiffViews) DropViewsOrdered(ofs output.OutputFileSegmenter, oldDoc *model.Definition, newDoc *model.Definition) {
	// TODO(go,pgsql)
}
