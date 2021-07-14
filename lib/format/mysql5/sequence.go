package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/model"
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

func (self *Sequence) GetMultiCreationSql(schema *model.Schema, sequences []*model.Sequence) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Sequence) GetMultiGrantSql(doc *model.Definition, schema *model.Schema, sequences []*model.Sequence) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Sequence) GetMultiRevokeSql(doc *model.Definition, schema *model.Schema, sequences []*model.Sequence) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}
