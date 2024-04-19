package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

type View struct {
}

func NewView() *View {
	return &View{}
}

func (self *View) GetGrantSql(doc *ir.Definition, schema *ir.Schema, view *ir.View, grant *ir.Grant) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *View) GetRevokeSql(doc *ir.Definition, schema *ir.Schema, view *ir.View, grant *ir.Grant) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}
