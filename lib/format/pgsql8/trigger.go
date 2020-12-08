package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
)

var GlobalTrigger *Trigger = NewTrigger()

type Trigger struct {
	IncludeColumnDefaultNextvalInCreateSql bool
}

func NewTrigger() *Trigger {
	return &Trigger{}
}

func (self *Trigger) GetCreationSql(schema *model.Schema, trigger *model.Trigger) []lib.ToSql {
	// TODO(go,pgsql)
	return nil
}
