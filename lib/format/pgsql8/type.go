package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
)

var GlobalDataType *DataType = NewDataType()

type DataType struct {
}

func NewDataType() *DataType {
	return &DataType{}
}

func (self *DataType) GetCreationSql(schema *model.Schema, datatype *model.DataType) []lib.ToSql {
	// TODO(go,pgsql)
	return nil
}
