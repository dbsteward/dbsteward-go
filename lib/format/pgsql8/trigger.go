package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Trigger struct {
	IncludeColumnDefaultNextvalInCreateSql bool
}

func NewTrigger() *Trigger {
	return &Trigger{}
}

func (self *Trigger) GetCreationSql(schema *model.Schema, trigger *model.Trigger) []output.ToSql {
	// TODO(go,3) move validation elsewhere
	if table := schema.TryGetTableNamed(trigger.Table); table == nil {
		lib.GlobalDBSteward.Fatal("Failed to find trigger table %s.%s", schema.Name, trigger.Table)
	}
	// TODO(feat) validate function exists

	if trigger.ForEach == "" {
		// TODO(feat) is it actually required?
		lib.GlobalDBSteward.Fatal("Trigger forEach must be defined for postgres trigger %s.%s", schema.Name, trigger.Name)
	}

	return []output.ToSql{
		&sql.TriggerCreate{
			Trigger:  sql.TriggerRef{schema.Name, trigger.Name},
			Table:    sql.TableRef{schema.Name, trigger.Table},
			Timing:   string(trigger.Timing),
			Events:   trigger.Events,
			ForEach:  string(trigger.ForEach),
			Function: trigger.Function,
		},
	}
}
