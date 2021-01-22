package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffTriggers struct {
}

func NewDiffTriggers() *DiffTriggers {
	return &DiffTriggers{}
}

func (self *DiffTriggers) DiffTriggers(ofs output.OutputFileSegmenter, oldSchema *model.Schema, newSchema *model.Schema) {
	for _, newTable := range newSchema.Tables {
		GlobalOperations.SetContextReplicaSetId(newTable.SlonySetId)
		oldTable := oldSchema.TryGetTableNamed(newTable.Name)
		self.DiffTriggersTable(ofs, oldSchema, oldTable, newSchema, newTable)
	}
}

func (self *DiffTriggers) DiffTriggersTable(ofs output.OutputFileSegmenter, oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) {
	if newTable == nil {
		// if newTable does not exist, existing triggers will have been implicitly dropped
		// and there cannot (should not?) be triggers for it
		return
	}

	if oldTable != nil {
		// drop old or changed triggers
		for _, oldTrigger := range oldSchema.GetTriggersForTableNamed(oldTable.Name) {
			if !oldTrigger.SqlFormat.Equals(model.SqlFormatPgsql8) {
				continue
			}
			newTrigger := newSchema.TryGetTriggerMatching(oldTrigger)
			if newTrigger == nil || !oldTrigger.Equals(newTrigger) {
				GlobalOperations.SetContextReplicaSetId(oldTrigger.SlonySetId)
				ofs.WriteSql(GlobalTrigger.GetDropSql(oldSchema, oldTrigger)...)
			}
		}
	}

	// create new or changed triggers
	for _, newTrigger := range newSchema.GetTriggersForTableNamed(newTable.Name) {
		if !newTrigger.SqlFormat.Equals(model.SqlFormatPgsql8) {
			continue
		}

		oldTrigger := oldSchema.TryGetTriggerMatching(newTrigger)
		if oldTrigger == nil || !oldTrigger.Equals(newTrigger) {
			GlobalOperations.SetContextReplicaSetId(oldTrigger.SlonySetId)
			ofs.WriteSql(GlobalTrigger.GetCreationSql(newSchema, newTrigger)...)
		}
	}
}
