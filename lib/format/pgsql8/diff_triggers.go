package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffTriggers struct {
}

func NewDiffTriggers() *DiffTriggers {
	return &DiffTriggers{}
}

func (self *DiffTriggers) DiffTriggers(ofs output.OutputFileSegmenter, oldSchema *ir.Schema, newSchema *ir.Schema) {
	for _, newTable := range newSchema.Tables {
		oldTable := oldSchema.TryGetTableNamed(newTable.Name)
		self.DiffTriggersTable(ofs, oldSchema, oldTable, newSchema, newTable)
	}
}

func (self *DiffTriggers) DiffTriggersTable(ofs output.OutputFileSegmenter, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) {
	if newTable == nil {
		// if newTable does not exist, existing triggers will have been implicitly dropped
		// and there cannot (should not?) be triggers for it
		return
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
			ofs.WriteSql(getCreateTriggerSql(newSchema, newTrigger)...)
		}
	}
}
