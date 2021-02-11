package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffIndexes struct {
}

func NewDiffIndexes() *DiffIndexes {
	return &DiffIndexes{}
}

func (self *DiffIndexes) DiffIndexes(ofs output.OutputFileSegmenter, oldSchema *model.Schema, newSchema *model.Schema) {
	for _, newTable := range newSchema.Tables {
		var oldTable *model.Table
		if oldSchema != nil {
			// TODO(feat) what about renames?
			oldTable = oldSchema.TryGetTableNamed(newTable.Name)
		}
		self.DiffIndexesTable(ofs, oldSchema, oldTable, newSchema, newTable)
	}
}

func (self *DiffIndexes) DiffIndexesTable(ofs output.OutputFileSegmenter, oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) {
	for _, oldIndex := range self.getOldIndexes(oldSchema, oldTable, newSchema, newTable) {
		// TODO(go,pgsql) old code used new schema/table instead of old, but I believe that is incorrect. need to verify this behavior change
		ofs.WriteSql(GlobalIndex.GetDropSql(oldSchema, oldTable, oldIndex)...)
	}

	// TODO(go,pgsql) old code used a different codepath if oldSchema = nil; need to verify this behavior change
	for _, newIndex := range self.getNewIndexes(oldSchema, oldTable, newSchema, newTable) {
		ofs.WriteSql(GlobalIndex.GetCreateSql(newSchema, newTable, newIndex)...)
	}
}

func (self *DiffIndexes) getOldIndexes(oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) []*model.Index {
	out := []*model.Index{}

	// if new table is nil we don't need to drop those indexes, they'll be dropped implicitly from the DROP TABLE
	// if old table is nil, we don't have any indexes to drop at all
	// TODO(go,4) do we want to consider recording the fact that the indexes dropped, and omit the DROP INDEX at the change-serialization phase?
	if oldTable != nil && newTable != nil {
		// we need to use pgsql8 getters and not model getters because we need to "hallucinate" column unique indexes.
		// we need to use format-specific Equals because what constitutes "equal" could theoretically change between formats
		// TODO(go,nth) move Equals to model if there's not actually any variation between formats
		// TODO(go,pgsql) this logic is slightly different than php. need to double check and test
		// TODO(go,3) we should move that hallucination to the compositing/expansion phase, and use plain old model getters here
		oldIndexes, err := GlobalIndex.GetTableIndexes(oldSchema, oldTable)
		lib.GlobalDBSteward.FatalIfError(err, "While finding old indexes")
		for _, oldIndex := range oldIndexes {
			newIndex, err := GlobalIndex.TryGetTableIndexNamed(newSchema, newTable, oldIndex.Name)
			lib.GlobalDBSteward.FatalIfError(err, "While finding new index corresponding to old")
			if newIndex == nil || !oldIndex.Equals(newIndex, model.SqlFormatPgsql8) {
				out = append(out, oldIndex)
			}
		}
	}

	return out
}

func (self *DiffIndexes) getNewIndexes(oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) []*model.Index {
	out := []*model.Index{}

	// if new table is nil, there _are_ no indexes to create
	// TODO(feat) detect index renames because renaming an index is almost certainly cheaper than re-indexing
	if newTable != nil {
		// TODO(go,pgsql) this logic is slightly different, make sure to test it
		newIndexes, err := GlobalIndex.GetTableIndexes(newSchema, newTable)
		lib.GlobalDBSteward.FatalIfError(err, "While finding new indexes")
		for _, newIndex := range newIndexes {
			oldIndex, err := GlobalIndex.TryGetTableIndexNamed(oldSchema, oldTable, newIndex.Name)
			lib.GlobalDBSteward.FatalIfError(err, "While finding old index corresponding to new")
			if oldIndex == nil || !oldIndex.Equals(newIndex, model.SqlFormatPgsql8) {
				out = append(out, newIndex)
			}
		}
	}

	return out
}
