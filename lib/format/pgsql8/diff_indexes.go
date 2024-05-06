package pgsql8

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

func diffIndexes(ofs output.OutputFileSegmenter, oldSchema *ir.Schema, newSchema *ir.Schema) error {
	for _, newTable := range newSchema.Tables {
		var oldTable *ir.Table
		if oldSchema != nil {
			// TODO(feat) what about renames?
			oldTable = oldSchema.TryGetTableNamed(newTable.Name)
		}
		err := diffIndexesTable(ofs, oldSchema, oldTable, newSchema, newTable)
		if err != nil {
			return err
		}
	}
	return nil
}

func diffIndexesTable(ofs output.OutputFileSegmenter, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) error {
	oldIndexes, err := getOldIndexes(oldSchema, oldTable, newSchema, newTable)
	if err != nil {
		return err
	}
	for _, oldIndex := range oldIndexes {
		// TODO(go,pgsql) old code used new schema/table instead of old, but I believe that is incorrect. need to verify this behavior change
		ofs.WriteSql(getDropIndexSql(oldSchema, oldIndex)...)
	}

	// TODO(go,pgsql) old code used a different codepath if oldSchema = nil; need to verify this behavior change
	newIndexes, err := getNewIndexes(oldSchema, oldTable, newSchema, newTable)
	if err != nil {
		return err
	}
	for _, newIndex := range newIndexes {
		ofs.WriteSql(getCreateIndexSql(newSchema, newTable, newIndex)...)
	}
	return nil
}

func getOldIndexes(oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) ([]*ir.Index, error) {
	out := []*ir.Index{}

	// if new table is nil we don't need to drop those indexes, they'll be dropped implicitly from the DROP TABLE
	// if old table is nil, we don't have any indexes to drop at all
	// TODO(go,4) do we want to consider recording the fact that the indexes dropped, and omit the DROP INDEX at the change-serialization phase?
	if oldTable != nil && newTable != nil {
		// we need to use pgsql8 getters and not model getters because we need to "hallucinate" column unique indexes.
		// we need to use format-specific Equals because what constitutes "equal" could theoretically change between formats
		// TODO(go,nth) move Equals to model if there's not actually any variation between formats
		// TODO(go,pgsql) this logic is slightly different than php. need to double check and test
		// TODO(go,3) we should move that hallucination to the compositing/expansion phase, and use plain old model getters here
		oldIndexes, err := getTableIndexes(oldSchema, oldTable)
		if err != nil {
			return nil, fmt.Errorf("while finding old indexes: %w", err)
		}
		for _, oldIndex := range oldIndexes {
			newIndex, err := tryGetTableIndexNamed(newSchema, newTable, oldIndex.Name)
			if err != nil {
				return nil, fmt.Errorf("while finding new index corresponding to old: %w", err)
			}
			if newIndex == nil || !oldIndex.Equals(newIndex, ir.SqlFormatPgsql8) {
				out = append(out, oldIndex)
			}
		}
	}

	return out, nil
}

func getNewIndexes(oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) ([]*ir.Index, error) {
	out := []*ir.Index{}

	// if new table is nil, there _are_ no indexes to create
	// TODO(feat) detect index renames because renaming an index is almost certainly cheaper than re-indexing
	if newTable != nil {
		// TODO(go,pgsql) this logic is slightly different, make sure to test it
		newIndexes, err := getTableIndexes(newSchema, newTable)
		if err != nil {
			return nil, fmt.Errorf("while finding new indexes: %w", err)
		}
		for _, newIndex := range newIndexes {
			oldIndex, err := tryGetTableIndexNamed(oldSchema, oldTable, newIndex.Name)
			if err != nil {
				return nil, fmt.Errorf("while finding old index corresponding to new: %w", err)
			}
			if oldIndex == nil || !oldIndex.Equals(newIndex, ir.SqlFormatPgsql8) {
				out = append(out, newIndex)
			}
		}
	}

	return out, nil
}
