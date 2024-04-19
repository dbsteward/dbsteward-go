package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/format/sql99"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffConstraints struct {
}

func NewDiffConstraints() *DiffConstraints {
	return &DiffConstraints{}
}

func (self *DiffConstraints) CreateConstraints(ofs output.OutputFileSegmenter, oldSchema, newSchema *ir.Schema, constraintType sql99.ConstraintType) {
	// TODO(go,mysql) implement me
}

func (self *DiffConstraints) DropConstraints(ofs output.OutputFileSegmenter, oldSchema, newSchema *ir.Schema, constraintType sql99.ConstraintType) {
	// TODO(go,mysql) implement me
}

func (self *DiffConstraints) CreateConstraintsTable(ofs output.OutputFileSegmenter, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table, constraintType sql99.ConstraintType) {
	// TODO(go,mysql) implement me
}

func (self *DiffConstraints) DropConstraintsTable(ofs output.OutputFileSegmenter, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table, constraintType sql99.ConstraintType) {
	// TODO(go,mysql) implement me
}
