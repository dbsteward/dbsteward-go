package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffTables struct {
}

func NewDiffTables() *DiffTables {
	return &DiffTables{}
}

func (self *DiffTables) DropTables(ofs output.OutputFileSegmenter, oldSchema, newSchema *ir.Schema) {
	// TODO(go,mysql) implement me
}

func (self *DiffTables) DiffTables(stage1, stage3 output.OutputFileSegmenter, oldSchema, newSchema *ir.Schema) {
	// TODO(go,mysql) implement me
}

func (self *DiffTables) DropTable(ofs output.OutputFileSegmenter, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema) {
	// TODO(go,mysql) implement me
}

func (self *DiffTables) CreateTable(ofs output.OutputFileSegmenter, oldSchema, newSchema *ir.Schema, newTable *ir.Table) error {
	// TODO(go,mysql) implement me
	return nil
}

func (self *DiffTables) DiffTable(stage1, stage3 output.OutputFileSegmenter, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) error {
	// TODO(go,mysql) implement me
	return nil
}

func (self *DiffTables) GetCreateDataSql(oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *DiffTables) GetDeleteDataSql(oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *DiffTables) DiffData(ofs output.OutputFileSegmenter, oldSchema, newSchema *ir.Schema) {
	// TODO(go,mysql) implement me
}
