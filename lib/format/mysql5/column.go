package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/model"
)

type Column struct {
}

func NewColumn() *Column {
	return &Column{}
}

func (self *Column) IsSerialType(column *model.Column) bool {
	// TODO(go,mysql) unify these
	return GlobalDataType.IsSerialType(column.Type)
}
