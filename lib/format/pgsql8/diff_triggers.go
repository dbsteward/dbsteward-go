package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

func diffTriggers(ofs output.OutputFileSegmenter, oldSchema *ir.Schema, newSchema *ir.Schema) error {
	for _, newTable := range newSchema.Tables {
		oldTable := oldSchema.TryGetTableNamed(newTable.Name)
		err := diffTriggersTable(ofs, oldSchema, oldTable, newSchema, newTable)
		if err != nil {
			return err
		}
	}
	return nil
}

func diffTriggersTable(ofs output.OutputFileSegmenter, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) error {
	if newTable == nil {
		// if newTable does not exist, existing triggers will have been implicitly dropped
		// and there cannot (should not?) be triggers for it
		return nil
	}

	if oldTable != nil {
		// drop old or changed triggers
		for _, oldTrigger := range oldSchema.GetTriggersForTableNamed(oldTable.Name) {
			if !oldTrigger.SqlFormat.Equals(ir.SqlFormatPgsql8) {
				continue
			}
			newTrigger := newSchema.TryGetTriggerMatching(oldTrigger)
			if newTrigger == nil || !oldTrigger.Equals(newTrigger) {
				ofs.WriteSql(getDropTriggerSql(oldSchema, oldTrigger)...)
			}
		}
	}

	// create new or changed triggers
	for _, newTrigger := range newSchema.GetTriggersForTableNamed(newTable.Name) {
		if !newTrigger.SqlFormat.Equals(ir.SqlFormatPgsql8) {
			continue
		}

		oldTrigger := oldSchema.TryGetTriggerMatching(newTrigger)
		if oldTrigger == nil || !oldTrigger.Equals(newTrigger) {
			s, err := getCreateTriggerSql(newSchema, newTrigger)
			if err != nil {
				return err
			}
			ofs.WriteSql(s...)
		}
	}
	return nil
}
