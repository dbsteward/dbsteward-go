package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Table struct {
}

func NewTable() *Table {
	return &Table{}
}

func (self *Table) GetSequencesNeeded(schema *ir.Schema, table *ir.Table) []*ir.Sequence {
	// TODO(go,mysql) implement me
	return nil
}
func (self *Table) GetTriggersNeeded(schema *ir.Schema, table *ir.Table) []*ir.Trigger {
	// TODO(go,mysql) implement me
	return nil
}
func (self *Table) GetCreationSql(schema *ir.Schema, table *ir.Table) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Table) GetDropSql(schema *ir.Schema, table *ir.Table) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Table) GetGrantSql(doc *ir.Definition, schema *ir.Schema, table *ir.Table, grant *ir.Grant) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Table) GetRevokeSql(doc *ir.Definition, schema *ir.Schema, table *ir.Table, grant *ir.Grant) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}
