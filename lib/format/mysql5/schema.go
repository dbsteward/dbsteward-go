package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Schema struct {
}

func NewSchema() *Schema {
	return &Schema{}
}
func (self *Schema) GetCreationSql(schema *ir.Schema) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Schema) GetDropSql(schema *ir.Schema) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Schema) GetGrantSql(doc *ir.Definition, schema *ir.Schema, grant *ir.Grant) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Schema) GetRevokeSql(doc *ir.Definition, schema *ir.Schema, grant *ir.Grant) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}
