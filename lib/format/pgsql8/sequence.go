package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

var GlobalSequence *Sequence = NewSequence()

type Sequence struct {
}

func NewSequence() *Sequence {
	return &Sequence{}
}

func (self *Sequence) GetCreationSql(schema *model.Schema, sequence *model.Sequence) []output.ToSql {
	// TODO(go,pgsql)
	return nil
}
