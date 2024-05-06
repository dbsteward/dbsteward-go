package pgsql8

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

func getCreateTriggerSql(schema *ir.Schema, trigger *ir.Trigger) ([]output.ToSql, error) {
	// TODO(go,3) move validation elsewhere
	if table := schema.TryGetTableNamed(trigger.Table); table == nil {
		return nil, fmt.Errorf("Failed to find trigger table %s.%s", schema.Name, trigger.Table)
	}
	// TODO(feat) validate function exists

	if trigger.ForEach == "" {
		// TODO(feat) is it actually required?
		return nil, fmt.Errorf("trigger forEach must be defined for postgres trigger %s.%s", schema.Name, trigger.Name)
	}

	return []output.ToSql{
		&sql.TriggerCreate{
			Trigger:  sql.TriggerRef{Schema: schema.Name, Trigger: trigger.Name},
			Table:    sql.TableRef{Schema: schema.Name, Table: trigger.Table},
			Timing:   string(trigger.Timing),
			Events:   trigger.Events,
			ForEach:  string(trigger.ForEach),
			Function: trigger.Function,
		},
	}, nil
}

func getDropTriggerSql(schema *ir.Schema, trigger *ir.Trigger) []output.ToSql {
	return []output.ToSql{
		&sql.TriggerDrop{
			Trigger: sql.TriggerRef{Schema: schema.Name, Trigger: trigger.Name},
			Table:   sql.TableRef{Schema: schema.Name, Table: trigger.Table},
		},
	}
}
