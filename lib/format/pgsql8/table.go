package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
)

var GlobalTable *Table = NewTable()

type Table struct {
	IncludeColumnDefaultNextvalInCreateSql bool
}

func NewTable() *Table {
	return &Table{}
}

func (self *Table) GetCreationSql(schema *model.Schema, table *model.Table) []lib.ToSql {
	// TODO(go,pgsql)
	return nil
}

func (self *Table) GetDefaultNextvalSql(schema *model.Schema, table *model.Table) []lib.ToSql {
	// TODO(go,pgsql)
	return nil
}

func (self *Table) DefineTableColumnDefaults(schema *model.Schema, table *model.Table) []lib.ToSql {
	// TODO(go,pgsql)
	return nil
}
