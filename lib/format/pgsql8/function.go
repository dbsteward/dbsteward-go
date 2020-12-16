package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

var GlobalFunction *Function = NewFunction()

type Function struct {
	IncludeColumnDefaultNextvalInCreateSql bool
}

func NewFunction() *Function {
	return &Function{}
}

func (self *Function) GetCreationSql(schema *model.Schema, function *model.Function) []output.ToSql {
	// TODO(go,pgsql)
	return nil
}
