package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Function struct {
}

func NewFunction() *Function {
	return &Function{}
}

func (self *Function) GetCreationSql(schema *model.Schema, function *model.Function) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Function) GetDropSql(schema *model.Schema, function *model.Function) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Function) GetGrantSql(doc *model.Definition, schema *model.Schema, fn *model.Function, grant *model.Grant) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}
