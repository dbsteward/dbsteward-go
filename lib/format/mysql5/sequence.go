package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

// TODO(go,mysql) Document the mysql sequence polyfill

type Sequence struct {
}

func NewSequence() *Sequence {
	return &Sequence{}
}

func (self *Sequence) GetShimCreationSql() []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Sequence) GetMultiCreationSql(schema *ir.Schema, sequences []*ir.Sequence) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Sequence) GetMultiGrantSql(doc *ir.Definition, schema *ir.Schema, sequences []*ir.Sequence) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Sequence) GetMultiRevokeSql(doc *ir.Definition, schema *ir.Schema, sequences []*ir.Sequence) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}
