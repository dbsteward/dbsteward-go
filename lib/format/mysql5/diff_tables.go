package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffTables struct {
}

func NewDiffTables() *DiffTables {
	return &DiffTables{}
}

func (self *DiffTables) DropTables(ofs output.OutputFileSegmenter, oldSchema, newSchema *model.Schema) {
	// TODO(go,mysql) implement me
}

func (self *DiffTables) DiffTables(stage1, stage3 output.OutputFileSegmenter, oldSchema, newSchema *model.Schema) {
	// TODO(go,mysql) implement me
}

func (self *DiffTables) DropTable(ofs output.OutputFileSegmenter, oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema) {
	// TODO(go,mysql) implement me
}

func (self *DiffTables) CreateTable(ofs output.OutputFileSegmenter, oldSchema, newSchema *model.Schema, newTable *model.Table) error {
	// TODO(go,mysql) implement me
	return nil
}

func (self *DiffTables) DiffTable(stage1, stage3 output.OutputFileSegmenter, oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) error {
	// TODO(go,mysql) implement me
	return nil
}

func (self *DiffTables) GetCreateDataSql(oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}
