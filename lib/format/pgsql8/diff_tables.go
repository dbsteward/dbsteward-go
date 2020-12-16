package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

var GlobalDiffTables *DiffTables = NewDiffTables()

type DiffTables struct {
}

func NewDiffTables() *DiffTables {
	return &DiffTables{}
}

func (self *DiffTables) DiffConstraints(ofs output.OutputFileSegmenter, oldSchema, newSchema *model.Schema, constraintType string, dropConstraints bool) {
	// TODO(go,pgsql)
}

func (self *DiffTables) DiffConstraintsTable(ofs output.OutputFileSegmenter, oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table, constraintType string, dropConstraints bool) {
	// TODO(go,pgsql)
}

func (self *DiffTables) DiffTables(stage1, stage3 output.OutputFileSegmenter, oldSchema, newSchema *model.Schema) {
	// TODO(go,pgsql)
}

func (self *DiffTables) DiffTable(stage1, stage3 output.OutputFileSegmenter, oldSchema, newSchema *model.Schema, oldTable, newTable *model.Table) {
	// TODO(go,pgsql)
}

func (self *DiffTables) DropTables(ofs output.OutputFileSegmenter, oldSchema, newSchema *model.Schema) {
	// TODO(go,pgsql)
}

func (self *DiffTables) DropTable(ofs output.OutputFileSegmenter, oldSchema, newSchema *model.Schema, oldTable, newTable *model.Table) {
	// TODO(go,pgsql)
}

func (self *DiffTables) DiffClusters(ofs output.OutputFileSegmenter, oldSchema, newSchema *model.Schema) {
	// TODO(go,pgsql)
}

func (self *DiffTables) DiffClustersTable(ofs output.OutputFileSegmenter, oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) {
	// TODO(go,pgsql)
}

func (self *DiffTables) GetDataSql(oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table, deleteMode bool) []output.ToSql {
	// TODO(go,pgsql)
	return nil
}
