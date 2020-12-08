package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
)

var GlobalSequence *Sequence = NewSequence()

type Sequence struct {
}

func NewSequence() *Sequence {
	return &Sequence{}
}

func (self *Sequence) GetCreationSql(schema *model.Schema, sequence *model.Sequence) []lib.ToSql {
	// TODO(go,pgsql)
	return nil
}
