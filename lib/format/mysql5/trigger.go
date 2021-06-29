package mysql5

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Trigger struct {
}

func NewTrigger() *Trigger {
	return &Trigger{}
}

func (self *Trigger) GetCreationSql(schema *model.Schema, trigger *model.Trigger) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}

func (self *Trigger) GetDropSql(schema *model.Schema, trigger *model.Trigger) []output.ToSql {
	// TODO(go,mysql) implement me
	return nil
}
