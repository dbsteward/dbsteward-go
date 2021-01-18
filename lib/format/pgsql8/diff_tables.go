package pgsql8

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

type DiffTables struct {
}

func NewDiffTables() *DiffTables {
	return &DiffTables{}
}

// TODO(go,core) lift much of this up to sql99

// applies transformations to tables that exist in both old and new
func (self *DiffTables) DiffTables(stage1, stage3 output.OutputFileSegmenter, oldSchema, newSchema *model.Schema) {
	// note: old dbsteward called create_tables here, but because we split out DiffTable, we can't call it both places,
	// so callers were updated to call CreateTables or CreateTable just before calling DiffTables or DiffTable, respectively

	if oldSchema == nil {
		return
	}
	for _, newTable := range newSchema.Tables {
		oldTable := oldSchema.TryGetTableNamed(newTable.Name)
		oldSchema, oldTable = lib.GlobalDBX.RenamedTableCheckPointer(oldSchema, oldTable, newSchema, newTable)
		self.DiffTable(stage1, stage3, oldSchema, oldTable, newSchema, newTable)
	}
}

func (self *DiffTables) DiffTable(stage1, stage3 output.OutputFileSegmenter, oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) {
	if oldTable == nil || newTable == nil {
		// create and drop are handled elsewhere
		return
	}

	self.updateTableOptions(stage1, oldSchema, oldTable, newSchema, newTable)
	self.updateTableColumns(stage1, stage3, oldTable, newSchema, newTable)
	self.checkPartition(oldTable, newTable)
	self.checkInherits(stage1, oldTable, newSchema, newTable)
	self.addAlterStatistics(stage1, oldTable, newSchema, newTable)
}

func (self *DiffTables) updateTableOptions(stage1 output.OutputFileSegmenter, oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) {
	util.Assert(oldTable != nil, "expect oldTable to not be nil")
	util.Assert(newTable != nil, "expect newTable to not be nil")

	oldOpts := oldTable.GetTableOptionStrMap(model.SqlFormatPgsql8)
	newOpts := newTable.GetTableOptionStrMap(model.SqlFormatPgsql8)

	// dropped options are those present in old table but not new
	deleteOpts := util.IDifferenceStrMapKeys(oldOpts, newOpts)

	// added options are those present in new table but not old
	createOpts := util.IDifferenceStrMapKeys(newOpts, oldOpts)

	// changed options are those present in both tables but with different values
	updateOpts := util.IntersectStrMapFunc(newOpts, oldOpts, func(newKey, oldKey string) bool {
		return strings.EqualFold(newKey, oldKey) && !strings.EqualFold(newOpts[newKey], oldOpts[oldKey])
	})

	self.applyTableOptionsDiff(stage1, newSchema, newTable, updateOpts, createOpts, deleteOpts)
}

func (self *DiffTables) applyTableOptionsDiff(stage1 output.OutputFileSegmenter, schema *model.Schema, table *model.Table, updateOpts, createOpts, deleteOpts map[string]string) {
	alters := []sql.TableAlterPart{}
	ref := sql.TableRef{schema.Name, table.Name}

	// in pgsql create and alter have the same syntax
	for name, value := range util.IUnionStrMapKeys(createOpts, updateOpts) {
		if strings.EqualFold(name, "with") {
			// ALTER TABLE ... SET (params) doesn't accept oids=true/false unlike CREATE TABLE
			// only WITH OIDS or WITHOUT OIDS
			params := GlobalTable.ParseStorageParams(value)
			if oids, ok := params["oids"]; ok {
				delete(params, "oids")
				if util.IsTruthy(oids) {
					alters = append(alters, &sql.TableAlterPartWithOids{})
				} else {
					alters = append(alters, &sql.TableAlterPartWithoutOids{})
				}
			} else {
				// we might have gotten rid of the oids param
				alters = append(alters, &sql.TableAlterPartWithoutOids{})
			}

			// set rest of params normally
			alters = append(alters, &sql.TableAlterPartSetStorageParams{params})
		} else if strings.EqualFold(name, "tablespace") {
			alters = append(alters, &sql.TableAlterPartSetTablespace{value})
			stage1.WriteSql(&sql.TableMoveTablespaceIndexes{
				Table:      ref,
				Tablespace: value,
			})
		} else {
			lib.GlobalDBSteward.Warning("Ignoring create/update of unknown table option %s on table %s.%s", name, schema.Name, table.Name)
		}
	}

	for name, value := range deleteOpts {
		if strings.EqualFold(name, "with") {
			params := GlobalTable.ParseStorageParams(value)
			// handle oids separately since pgsql doesn't recognize it as a storage parameter in an ALTER TABLE
			if _, ok := params["oids"]; ok {
				delete(params, "oids")
				alters = append(alters, &sql.TableAlterPartWithoutOids{})
			}
			// handle rest normally
			alters = append(alters, &sql.TableAlterPartResetStorageParams{util.StrMapKeys(params)})
		} else if strings.EqualFold(name, "tablespace") {
			stage1.WriteSql(&sql.TableResetTablespace{
				Table: ref,
			})
		} else {
			lib.GlobalDBSteward.Warning("Ignoring removal of unknown table option %s on table %s.%s", name, schema.Name, table.Name)
		}
	}

	if len(alters) > 0 {
		stage1.WriteSql(&sql.TableAlterParts{
			Table: ref,
			Parts: alters,
		})
	}
}

type updateTableColumnsAgg struct {
	before1          []output.ToSql
	before3          []output.ToSql
	stage1           []sql.TableAlterPart
	stage3           []sql.TableAlterPart
	after1           []output.ToSql
	after3           []output.ToSql
	dropDefaultsCols []string
}

func (self *DiffTables) updateTableColumns(stage1, stage3 output.OutputFileSegmenter, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) {
	agg := &updateTableColumnsAgg{}

	// TODO(go,pgsql) old dbsteward interleaved commands into a single list, and output in the same order
	// meaning that a BEFORE3 could be output before a BEFORE1. in this implementation, _all_ BEFORE1s
	// are printed before BEFORE3s. Double check that this doesn't break anything.

	self.addDropTableColumns(agg, oldTable, newTable)
	self.addCreateTableColumns(agg, oldTable, newSchema, newTable)
	self.addModifyTableColumns(agg, oldTable, newSchema, newTable)

	// Note: in the case of single stage upgrades, stage1==stage3, so do all the Before's before all of the stages, and do them in stage order
	stage1.WriteSql(agg.before1...)
	stage3.WriteSql(agg.before3...)

	ref := sql.TableRef{newSchema.Name, newTable.Name}
	if newTable.SlonyId != nil {
		// slony will make the alter table statement changes as its super user
		// which if the db owner is different,
		// implicit sequence creation will fail with:
		// ERROR:  55000: sequence must have same owner as table it is linked to
		// so if the alter statement contains a new serial column,
		// change the user to the slony user for the alter, then (see similar block below)

		// TODO
	}
	stage1.WriteSql(&sql.TableAlterParts{
		Table: ref,
		Parts: agg.stage1,
	})
	if newTable.SlonyId != nil {
		// replicated table? put ownership back

		// TODO
	}

	stage3.WriteSql(&sql.TableAlterParts{
		Table: ref,
		Parts: agg.stage3,
	})

	defaultDrops := make([]sql.TableAlterPart, len(agg.dropDefaultsCols))
	for i, col := range agg.dropDefaultsCols {
		defaultDrops[i] = &sql.TableAlterPartColumnDropDefault{col}
	}
	stage1.WriteSql(&sql.TableAlterParts{ref, defaultDrops})

	stage1.WriteSql(agg.after1...)
	stage3.WriteSql(agg.after3...)
}

func (self *DiffTables) addDropTableColumns(agg *updateTableColumnsAgg, oldTable, newTable *model.Table) {
	for _, oldColumn := range oldTable.Columns {
		if newTable.TryGetColumnNamed(oldColumn.Name) != nil {
			// new column exists, not dropping it
			continue
		}

		renamedColumn := newTable.TryGetColumnOldNamed(oldColumn.Name)
		if !lib.GlobalDBSteward.IgnoreOldNames && renamedColumn != nil {
			agg.after3 = append(agg.after3, sql.NewComment(
				"%s DROP COLUMN %s omitted: new column %s indicates it is the replacement for %s",
				oldTable.Name, oldColumn.Name, renamedColumn.Name, oldColumn.Name,
			))
		} else {
			agg.stage3 = append(agg.stage3, &sql.TableAlterPartColumnDrop{oldColumn.Name})
		}
	}
}

func (self *DiffTables) addCreateTableColumns(agg *updateTableColumnsAgg, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) {

}

func (self *DiffTables) addModifyTableColumns(agg *updateTableColumnsAgg, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) {

}

func (self *DiffTables) checkPartition(oldTable, newTable *model.Table) {

}

func (self *DiffTables) checkInherits(stage1 output.OutputFileSegmenter, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) {

}

func (self *DiffTables) addAlterStatistics(stage1 output.OutputFileSegmenter, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) {

}

func (self *DiffTables) IsRenamedTable(schema *model.Schema, table *model.Table) bool {
	util.Assert(!lib.GlobalDBSteward.IgnoreOldNames, "should check IgnoreOldNames before calling IsRenamedTable")
	if table.OldTableName == "" {
		return false
	}
	if schema.TryGetTableNamed(table.OldTableName) != nil {
		// TODO(feat) what if the table moves schemas?
		// TODO(feat) what if we move a table and replace it with a table of the same name?
		lib.GlobalDBSteward.Fatal("oldTableName panic - new schema %s still contains table named %s", schema.Name, table.OldTableName)
	}

	oldSchema := GlobalTable.GetOldTableSchema(schema, table)
	if oldSchema != nil {
		if oldSchema.TryGetTableNamed(table.OldTableName) == nil {
			lib.GlobalDBSteward.Fatal("oldTableName panic - old schema %s does not contain table named %s", oldSchema.Name, table.OldTableName)
		}
	}

	// it is a new old named table rename if:
	// table.OldTableName exists in old schema
	// table.OldTableName does not exist in new schema
	if oldSchema.TryGetTableNamed(table.OldTableName) != nil && schema.TryGetTableNamed(table.OldTableName) == nil {
		lib.GlobalDBSteward.Info("Table %s used to be called %s", table.Name, table.OldTableName)
		return true
	}
	return false
}

func (self *DiffTables) CreateTables(ofs output.OutputFileSegmenter, oldSchema, newSchema *model.Schema) {
	if newSchema == nil {
		// if the new schema is nil, there's no tables to create
		return
	}
	for _, newTable := range newSchema.Tables {
		self.CreateTable(ofs, oldSchema, newSchema, newTable)
	}
}

func (self *DiffTables) CreateTable(ofs output.OutputFileSegmenter, oldSchema, newSchema *model.Schema, newTable *model.Table) {
	if newTable == nil {
		// TODO(go,nth) we shouldn't be here? should this be an Assert?
		return
	}
	if oldSchema.TryGetTableNamed(newTable.Name) != nil {
		// old table exists, alters or drops will be handled by other code
		return
	}

	if !lib.GlobalDBSteward.IgnoreOldNames && self.IsRenamedTable(newSchema, newTable) {
		// this is a renamed table, so rename it instead of creating a new one
		oldTableSchema := GlobalTable.GetOldTableSchema(newSchema, newTable)
		oldTable := GlobalTable.GetOldTable(newSchema, newTable)

		// ALTER TABLE ... RENAME TO does not accept schema qualifiers ...
		oldRef := sql.TableRef{oldTableSchema.Name, oldTable.Name}
		ofs.WriteSql(&sql.Annotated{
			Annotation: "table rename from oldTableName specification",
			Wrapped: &sql.TableAlterRename{
				Table:   oldRef,
				NewName: newTable.Name,
			},
		})
		// ... so if the schema changes issue a SET SCHEMA
		if !strings.EqualFold(oldTableSchema.Name, newSchema.Name) {
			ofs.WriteSql(&sql.Annotated{
				Annotation: "table reschema from oldSchemaName specification",
				Wrapped: &sql.TableAlterSetSchema{
					Table:     oldRef,
					NewSchema: newSchema.Name,
				},
			})
		}
	} else {
		ofs.WriteSql(GlobalTable.GetCreationSql(newSchema, newTable)...)
		ofs.WriteSql(GlobalTable.DefineTableColumnDefaults(newSchema, newTable)...)
	}
}

func (self *DiffTables) DropTables(ofs output.OutputFileSegmenter, oldSchema, newSchema *model.Schema) {
	// if newSchema is nil, we'll have already dropped all the tables in it
	if oldSchema != nil && newSchema != nil {
		for _, oldTable := range oldSchema.Tables {
			self.DropTable(ofs, oldSchema, oldTable, newSchema)
		}
	}
}

func (self *DiffTables) DropTable(ofs output.OutputFileSegmenter, oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema) {
	newTable := newSchema.TryGetTableNamed(oldTable.Name)
	if newTable != nil {
		// table exists, nothing to do
		return
	}
	if !lib.GlobalDBSteward.IgnoreOldNames {
		renamedRef := lib.GlobalDBX.TryGetTableFormerlyKnownAs(lib.GlobalDBSteward.NewDatabase, oldSchema, oldTable)
		if renamedRef != nil {
			ofs.Write("-- DROP TABLE %s.%s omitted: new table %s indicates it is her replacement", oldSchema.Name, oldTable.Name, renamedRef)
			return
		}
	}

	ofs.WriteSql(GlobalTable.GetDropSql(oldSchema, oldTable)...)
}

func (self *DiffTables) DiffClusters(ofs output.OutputFileSegmenter, oldSchema, newSchema *model.Schema) {
	// TODO(go,pgsql)
}

func (self *DiffTables) DiffClustersTable(ofs output.OutputFileSegmenter, oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) {
	// TODO(go,pgsql)
}

func (self *DiffTables) GetCreateDataSql(oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) []output.ToSql {
	newRows, updatedRows := self.getNewAndChangedRows(oldTable, newTable)
	// cut back on allocations - we know that there's going to be _at least_ one statement for every new and updated row, and likely 1 for the serial start
	out := make([]output.ToSql, 0, len(newRows)+len(updatedRows)+1)

	for _, updatedRow := range updatedRows {
		out = append(out, self.buildDataUpdate(newSchema, newTable, updatedRow))
	}
	for _, newRow := range newRows {
		// TODO(go,3) batch inserts
		out = append(out, self.buildDataInsert(newSchema, newTable, newRow))
	}

	if oldTable == nil {
		// if this is a fresh build, make sure serial starts are issued _after_ the hardcoded data inserts
		out = append(out, GlobalTable.GetSerialStartDml(newSchema, newTable)...)
		return out
	}

	return out
}

func (self *DiffTables) GetDeleteDataSql(oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) []output.ToSql {
	oldRows := self.getOldRows(oldTable, newTable)
	out := make([]output.ToSql, len(oldRows))
	for i, oldRow := range oldRows {
		out[i] = self.buildDataDelete(oldSchema, oldTable, oldRow)
	}
	return out
}

// TODO(go,3) all these row diffing functions feel awkward and too involved, let's see if we can't revisit these

// returns the rows in newTable which are new or updated, respectively, relative to oldTable
// TODO(go,3) move this to model
type changedRow struct {
	oldCols []string
	oldRow  *model.DataRow
	newRow  *model.DataRow
}

func (self *DiffTables) getNewAndChangedRows(oldTable, newTable *model.Table) ([]*model.DataRow, []*changedRow) {
	// TODO(go,pgsql) consider DataRow.Delete
	if newTable == nil || newTable.Rows == nil || len(newTable.Rows.Rows) == 0 || len(newTable.Rows.Columns) == 0 {
		// there are no new rows at all, so nothing is new or changed
		return nil, nil
	}

	if oldTable == nil || oldTable.Rows == nil || len(oldTable.Rows.Rows) == 0 || len(oldTable.Rows.Columns) == 0 {
		// there are no old rows at all, so everything is new, nothing is changed
		newRows := make([]*model.DataRow, len(newTable.Rows.Rows))
		copy(newRows, newTable.Rows.Rows)
		return newRows, nil
	}

	newRows := []*model.DataRow{}
	updatedRows := []*changedRow{}
	for _, newRow := range newTable.Rows.Rows {
		oldRow := oldTable.Rows.TryGetRowMatchingKeyCols(newRow, newTable.PrimaryKey)
		if oldRow == nil {
			newRows = append(newRows, newRow)
		} else if !newTable.Rows.RowEquals(newRow, oldRow, oldTable.Rows.Columns) {
			updatedRows = append(updatedRows, &changedRow{
				oldCols: oldTable.Rows.Columns,
				oldRow:  oldRow,
				newRow:  newRow,
			})
		}
	}
	return newRows, updatedRows
}

// returns the rows in oldTable that are no longer in newTable
// TODO(go,3) move this to model
func (self *DiffTables) getOldRows(oldTable, newTable *model.Table) []*model.DataRow {
	// TODO(go,pgsql) consider DataRow.Delete
	if oldTable == nil || oldTable.Rows == nil || len(oldTable.Rows.Rows) == 0 || len(oldTable.Rows.Columns) == 0 {
		// there are no old rows at all
		return nil
	}
	if newTable == nil || newTable.Rows == nil || len(newTable.Rows.Rows) == 0 || len(newTable.Rows.Columns) == 0 {
		// there are no new rows at all, so everything is old
		oldRows := make([]*model.DataRow, len(oldTable.Rows.Rows))
		copy(oldRows, oldTable.Rows.Rows)
		return oldRows
	}

	oldRows := []*model.DataRow{}
	for _, oldRow := range oldTable.Rows.Rows {
		// NOTE: we use new primary key here, because new is new, baby
		newRow := newTable.Rows.TryGetRowMatchingKeyCols(oldRow, newTable.PrimaryKey)
		if newRow == nil {
			oldRows = append(oldRows, oldRow)
		}
		// don't bother checking for changes, that's handled by getNewAndUpdatedRows in a completely different codepath
	}
	return oldRows
}

func (self *DiffTables) buildDataInsert(schema *model.Schema, table *model.Table, row *model.DataRow) output.ToSql {
	util.Assert(table.Rows != nil, "table.Rows should not be nil when calling buildDataInsert")
	util.Assert(!row.Delete, "do not call buildDataInsert for a row marked for deletion")
	values := make([]string, len(row.Columns))
	for i, col := range table.Rows.Columns {
		values[i] = GlobalOperations.ColumnValueDefault(schema, table, col, row.Columns[i])
	}
	return &sql.DataInsert{
		Table:   sql.TableRef{schema.Name, table.Name},
		Columns: table.Rows.Columns,
		Values:  values,
	}
}

func (self *DiffTables) buildDataUpdate(schema *model.Schema, table *model.Table, change *changedRow) output.ToSql {
	// TODO(feat) deal with column renames
	util.Assert(table.Rows != nil, "table.Rows should not be nil when calling buildDataUpdate")
	util.Assert(!change.newRow.Delete, "do not call buildDataUpdate for a row marked for deletion")

	updateCols := []string{}
	updateVals := []string{}
	for i, newCol := range change.newRow.Columns {
		newColName := table.Rows.Columns[i]

		oldColIdx := util.IIndexOfStr(newColName, change.oldCols)
		if oldColIdx < 0 {
			lib.GlobalDBSteward.Fatal("Could not compare rows: could not find column %s in table %s.%s <rows columns>", newColName, schema.Name, table.Name)
		}
		oldCol := change.oldRow.Columns[oldColIdx]

		if !oldCol.Equals(newCol) {
			updateCols = append(updateCols, newColName)
			updateVals = append(updateVals, GlobalOperations.ColumnValueDefault(schema, table, newColName, newCol))
		}
	}

	keyVals := []string{}
	pkCols, ok := table.Rows.TryGetColsMatchingKeyCols(change.newRow, table.PrimaryKey)
	if !ok {
		lib.GlobalDBSteward.Fatal("Could not compare rows: could not find primary key columns %v in <rows columns=%v> in table %s.%s", table.PrimaryKey, table.Rows.Columns, schema.Name, table.Name)
	}
	for i, pkCol := range pkCols {
		// TODO(go,pgsql) orig code in dbx::primary_key_expression uses `format::value_escape`, but that doesn't account for null, empty, sql, etc
		keyVals = append(keyVals, GlobalOperations.ColumnValueDefault(schema, table, table.PrimaryKey[i], pkCol))
	}

	return &sql.DataUpdate{
		Table:          sql.TableRef{schema.Name, table.Name},
		UpdatedColumns: updateCols,
		UpdatedValues:  updateVals,
		KeyColumns:     table.PrimaryKey,
		KeyValues:      keyVals,
	}
}

func (self *DiffTables) buildDataDelete(schema *model.Schema, table *model.Table, row *model.DataRow) output.ToSql {
	keyVals := []string{}
	pkCols, ok := table.Rows.TryGetColsMatchingKeyCols(row, table.PrimaryKey)
	if !ok {
		lib.GlobalDBSteward.Fatal("Could not compare rows: could not find primary key columns %v in <rows columns=%v> in table %s.%s", table.PrimaryKey, table.Rows.Columns, schema.Name, table.Name)
	}
	for i, pkCol := range pkCols {
		// TODO(go,pgsql) orig code in dbx::primary_key_expression uses `format::value_escape`, but that doesn't account for null, empty, sql, etc
		keyVals = append(keyVals, GlobalOperations.ColumnValueDefault(schema, table, table.PrimaryKey[i], pkCol))
	}
	return &sql.DataDelete{
		Table:   sql.TableRef{schema.Name, table.Name},
		Columns: table.PrimaryKey,
		Values:  keyVals,
	}
}
