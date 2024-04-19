package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Trigger struct {
}

func NewTrigger() *Trigger {
	return &Trigger{}
}

func (self *Trigger) GetCreationSql(schema *ir.Schema, trigger *ir.Trigger) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Trigger) GetDropSql(schema *ir.Schema, trigger *ir.Trigger) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}
