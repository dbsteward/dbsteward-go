package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/format/sql99"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffConstraints struct {
}

func NewDiffConstraints() *DiffConstraints {
	return &DiffConstraints{}
}

func (self *DiffConstraints) CreateConstraintsTable(ofs output.OutputFileSegmenter, oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table, constraintType sql99.ConstraintType) {
	// TODO(go,mysql) implement me
}
