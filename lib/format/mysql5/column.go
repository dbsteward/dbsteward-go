package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/ir"
)

type Column struct {
}

func NewColumn() *Column {
	return &Column{}
}

func (self *Column) IsSerialType(column *ir.Column) bool {
	// TODO(go,mysql) unify these
	return GlobalDataType.IsSerialType(column.Type)
}
