package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

var GlobalDiffSequences *DiffSequences = NewDiffSequences()

type DiffSequences struct {
}

func NewDiffSequences() *DiffSequences {
	return &DiffSequences{}
}

func (self *DiffSequences) DiffSequences(ofs output.OutputFileSegmenter, oldSchema *model.Schema, newSchema *model.Schema) {
	// TODO(go,pgsql)
}
