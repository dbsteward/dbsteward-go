package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Function struct {
}

func NewFunction() *Function {
	return &Function{}
}

func (self *Function) GetCreationSql(schema *ir.Schema, function *ir.Function) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Function) GetDropSql(schema *ir.Schema, function *ir.Function) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Function) GetGrantSql(doc *ir.Definition, schema *ir.Schema, fn *ir.Function, grant *ir.Grant) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Function) GetRevokeSql(doc *ir.Definition, schema *ir.Schema, fn *ir.Function, grant *ir.Grant) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}
