package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

var GlobalColumn *Column = NewColumn()

func NewColumn() *Column {
	return &Column{}
}

type Column struct {
}

func (self *Column) GetReducedDefinition(doc *model.Definition, schema *model.Schema, table *model.Table, column *model.Column) sql.CreateTableColumn {
	// TODO(go,pgsql)
	return sql.CreateTableColumn{}
}

func (self *Column) GetSetupSql(schema *model.Schema, table *model.Table, column *model.Column) []output.ToSql {
	// TODO(go,pgsql)
	return nil
}

func (self *Column) IsSerialType(column *model.Column) bool {
	return util.IIndexOfStr(column.Type, []string{"serial", "bigserial"}) >= 0
}
