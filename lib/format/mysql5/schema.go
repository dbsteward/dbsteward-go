package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Schema struct {
}

func NewSchema() *Schema {
	return &Schema{}
}
func (self *Schema) GetCreationSql(schema *model.Schema) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Schema) GetDropSql(schema *model.Schema) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Schema) GetGrantSql(doc *model.Definition, schema *model.Schema, grant *model.Grant) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}
