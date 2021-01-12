package pgsql8

import (
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

func (self *DiffTables) DiffTables(stage1, stage3 output.OutputFileSegmenter, oldSchema, newSchema *model.Schema) {
	// TODO(go,pgsql)
}

func (self *DiffTables) DiffTable(stage1, stage3 output.OutputFileSegmenter, oldSchema, newSchema *model.Schema, oldTable, newTable *model.Table) {
	// TODO(go,pgsql)
}

func (self *DiffTables) IsRenamedTable(schema *model.Schema, table *model.Table) bool {
	// TODO(go,pgsql)
	return false
}

func (self *DiffTables) DropTables(ofs output.OutputFileSegmenter, oldSchema, newSchema *model.Schema) {
	// TODO(go,pgsql)
}

func (self *DiffTables) DropTable(ofs output.OutputFileSegmenter, oldSchema, newSchema *model.Schema, oldTable, newTable *model.Table) {
	// TODO(go,pgsql)
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
