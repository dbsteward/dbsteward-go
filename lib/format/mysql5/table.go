package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Table struct {
}

func NewTable() *Table {
	return &Table{}
}

func (self *Table) GetSequencesNeeded(schema *model.Schema, table *model.Table) []*model.Sequence {
	// TODO(go,mysql) implement me
	return nil
}
func (self *Table) GetTriggersNeeded(schema *model.Schema, table *model.Table) []*model.Trigger {
	// TODO(go,mysql) implement me
	return nil
}
func (self *Table) GetCreationSql(schema *model.Schema, table *model.Table) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Table) GetDropSql(schema *model.Schema, table *model.Table) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Table) GetGrantSql(doc *model.Definition, schema *model.Schema, table *model.Table, grant *model.Grant) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}
