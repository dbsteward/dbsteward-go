package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

var GlobalSchema *Schema = NewSchema()

type Schema struct {
}

func NewSchema() *Schema {
	return &Schema{}
}

func (self *Schema) GetCreationSql(schema *model.Schema) []output.ToSql {
	// TODO(go,pgsql)
	return nil
}
