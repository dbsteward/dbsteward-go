package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
)

var GlobalSchema *Schema = NewSchema()

type Schema struct {
}

func NewSchema() *Schema {
	return &Schema{}
}

func (self *Schema) GetCreationSql(schema *model.Schema) []lib.ToSql {
	// TODO(go,pgsql)
	return nil
}
