package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DataType struct {
}

func NewDataType() *DataType {
	return &DataType{}
}

func (self *DataType) GetCreationSql(schema *model.Schema, datatype *model.DataType) ([]output.ToSql, error) {
	// TODO(go,mysql) implement me
	return nil, nil
}
