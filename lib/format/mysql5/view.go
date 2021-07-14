package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type View struct {
}

func NewView() *View {
	return &View{}
}

func (self *View) GetGrantSql(doc *model.Definition, schema *model.Schema, view *model.View, grant *model.Grant) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *View) GetRevokeSql(doc *model.Definition, schema *model.Schema, view *model.View, grant *model.Grant) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}
