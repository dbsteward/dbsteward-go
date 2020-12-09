package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
)

var GlobalDiffTables *DiffTables = NewDiffTables()

type DiffTables struct {
}

func NewDiffTables() *DiffTables {
	return &DiffTables{}
}

func (self *DiffTables) DiffConstraintsTable(ofs lib.OutputFileSegmenter, oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table, constraintType string, dropConstraints bool) {
	// TODO(go,pgsql)
}

func (self *DiffTables) GetDataSql(oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table, deleteMode bool) []lib.ToSql {
	// TODO(go,pgsql)
	return nil
}
