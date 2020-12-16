package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

var GlobalDataType *DataType = NewDataType()

type DataType struct {
}

func NewDataType() *DataType {
	return &DataType{}
}

func (self *DataType) GetCreationSql(schema *model.Schema, datatype *model.DataType) []output.ToSql {
	// TODO(go,pgsql)
	return nil
}

func (self *DataType) IsLinkedTableType(spec string) bool {
	// TODO(go,pgsql) see pgsql8::PATTERN_TABLE_LINKED_TYPES
	return false
}
