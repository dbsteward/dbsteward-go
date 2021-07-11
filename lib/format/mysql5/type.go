package mysql5

import (
	"strings"

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

func (self *DataType) IsLinkedTableType(spec string) bool {
	// TODO(go,mysql) unify these
	return self.IsSerialType(spec)
}

func (self *DataType) IsSerialType(spec string) bool {
	return strings.EqualFold(spec, DataTypeSerial) || strings.EqualFold(spec, DataTypeBigSerial)
}
