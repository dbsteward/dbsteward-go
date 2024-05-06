package xml

import (
	"log/slog"
	"strings"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/pkg/errors"
)

type DataRows struct {
	TabRowDelimiter string        `xml:"tabrowDelimiter,attr,omitempty"`
	Columns         DelimitedList `xml:"columns,attr,omitempty"`
	Rows            []*DataRow    `xml:"row"`
	TabRows         []string      `xml:"tabrow"`
}

func DataRowsFromIR(l *slog.Logger, rows *ir.DataRows) (*DataRows, error) {
	if rows == nil {
		return nil, nil
	}
	rv := DataRows{
		TabRowDelimiter: rows.TabRowDelimiter,
		Columns:         rows.Columns,
	}
	rv.TabRows = append(rv.TabRows, rows.TabRows...)
	for _, row := range rows.Rows {
		if row != nil {
			rv.Rows = append(
				rv.Rows,
				&DataRow{
					Columns: DataColFromIR(l, row.Columns),
					Delete:  row.Delete,
				},
			)
		}
	}
	return &rv, nil
}

func DataColFromIR(l *slog.Logger, cols []*ir.DataCol) []*DataCol {
	var rv []*DataCol
	for _, col := range cols {
		if col != nil {
			rv = append(
				rv,
				&DataCol{
					Null:  col.Null,
					Empty: col.Empty,
					Sql:   col.Sql,
					Text:  col.Text,
				},
			)
		}
	}
	return rv
}

func (dr *DataRows) ToIR() (*ir.DataRows, error) {
	if dr == nil {
		return nil, nil
	}
	rv := ir.DataRows{
		TabRowDelimiter: dr.TabRowDelimiter,
		Columns:         dr.Columns,
		TabRows:         dr.TabRows,
	}
	for _, row := range dr.Rows {
		nRow, err := row.ToIR()
		if err != nil {
			return nil, err
		}
		rv.Rows = append(rv.Rows, nRow)
	}
	return &rv, nil
}

type DataRow struct {
	Columns []*DataCol `xml:"col"`
	Delete  bool       `xml:"delete,attr,omitempty"` // TODO(go,core) does this un/marshal properly?
}

func (dr *DataRow) ToIR() (*ir.DataRow, error) {
	if dr == nil {
		return nil, nil
	}
	rv := ir.DataRow{
		Delete: dr.Delete,
	}
	for _, dc := range dr.Columns {
		nDC, err := dc.ToIR()
		if err != nil {
			return nil, err
		}
		rv.Columns = append(rv.Columns, nDC)
	}
	return &rv, nil
}

type DataCol struct {
	Null  bool   `xml:"null,attr,omitempty"`
	Empty bool   `xml:"empty,attr,omitempty"`
	Sql   bool   `xml:"sql,attr,omitempty"`
	Text  string `xml:",chardata"`
}

func (dc *DataCol) ToIR() (*ir.DataCol, error) {
	if dc == nil {
		return nil, nil
	}
	rv := ir.DataCol{
		Null:  dc.Null,
		Empty: dc.Empty,
		Sql:   dc.Sql,
		Text:  dc.Text,
	}
	return &rv, nil
}

func (drs *DataRows) AddColumn(name string, value string) error {
	if drs.HasColumn(name) {
		return errors.Errorf("already has column %s", name)
	}
	drs.Columns = append(drs.Columns, name)
	for _, row := range drs.Rows {
		// TODO(feat) what about nulls?
		row.Columns = append(row.Columns, &DataCol{Text: value})
	}
	return nil
}

func (drs *DataRows) HasColumn(name string) bool {
	for _, col := range drs.Columns {
		if col == name {
			return true
		}
	}
	return false
}

// Replaces TabRows with Rows
func (drs *DataRows) ConvertTabRows() {
	delimiter := util.CoalesceStr(drs.TabRowDelimiter, "\t")
	delimiter = strings.ReplaceAll(delimiter, "\\t", "\t")
	delimiter = strings.ReplaceAll(delimiter, "\\n", "\n")
	drs.TabRowDelimiter = ""

	for _, tabrow := range drs.TabRows {
		tabcols := strings.Split(tabrow, delimiter)
		row := &DataRow{
			Columns: make([]*DataCol, len(tabcols)),
		}

		for i, col := range tabcols {
			// similar to pgsql \N notation, make the column value explicitly null
			if col == `\N` {
				row.Columns[i] = &DataCol{Null: true}
			} else {
				row.Columns[i] = &DataCol{Text: col}
			}
		}
	}
	drs.TabRows = nil
}

func (drs *DataRows) GetColMap(row *DataRow) map[string]*DataCol {
	return drs.GetColMapKeys(row, drs.Columns)
}

func (drs *DataRows) GetColMapKeys(row *DataRow, keys []string) map[string]*DataCol {
	out := map[string]*DataCol{}
	for i, col := range row.Columns {
		if util.IStrsContains(keys, drs.Columns[i]) {
			out[drs.Columns[i]] = col
		}
	}
	return out
}

func (drs *DataRows) TryGetRowMatchingColMap(colmap map[string]*DataCol) *DataRow {
	for _, row := range drs.Rows {
		if drs.RowMatchesColMap(row, colmap) {
			return row
		}
	}
	return nil
}

// `row` matches `colmap` if all the columns in colmap match a column in the row
func (drs *DataRows) RowMatchesColMap(row *DataRow, colmap map[string]*DataCol) bool {
	for colName, col := range colmap {
		// find the corresponding column
		idx := util.IStrsIndex(drs.Columns, colName)
		if idx < 0 {
			return false // the column doesn't exist in this DataRows
		}

		rowCol := row.Columns[idx]
		if !rowCol.Equals(col) {
			return false
		}
	}
	return true
}

func (drs *DataRows) tryGetColIndexesOfNames(names []string) ([]int, bool) {
	out := make([]int, len(names))
	for i, name := range names {
		found := false
		for j, col := range drs.Columns {
			if strings.EqualFold(name, col) {
				out[i] = j
				found = true
				break
			}
		}
		if !found {
			return nil, false
		}
	}
	return out, true
}

// checks to see that ownRow == otherRow, accounting for possible differences in column count or order
func (drs *DataRows) RowEquals(ownRow, otherRow *DataRow, otherColumns []string) bool {
	if len(drs.Columns) != len(otherColumns) {
		return false
	}

	if ownRow.Delete != otherRow.Delete {
		return false
	}

	otherIndexes, ok := drs.tryGetColIndexesOfNames(otherColumns)
	if !ok {
		return false
	}

	for ownIndex, otherIndex := range otherIndexes {
		if !ownRow.Columns[ownIndex].Equals(otherRow.Columns[otherIndex]) {
			return false
		}
	}

	return false
}

func (dc *DataCol) Equals(other *DataCol) bool {
	if dc == nil || other == nil {
		return false
	}
	if dc.Null && other.Null {
		return true
	}
	if dc.Empty && other.Empty {
		return true
	}
	if dc.Sql != other.Sql {
		return false
	}
	// TODO(feat) something other than plain old string equality?
	return dc.Text == other.Text
}
