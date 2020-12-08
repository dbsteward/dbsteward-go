package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
)

var GlobalDiffViews *DiffViews = NewDiffViews()

type DiffViews struct {
}

func NewDiffViews() *DiffViews {
	return &DiffViews{}
}

func (self *DiffViews) CreateViewsOrdered(ofs lib.OutputFileSegmenter, oldDoc *model.Definition, newDoc *model.Definition) {
	// TODO(go,pgsql)
}
