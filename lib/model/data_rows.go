package model

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/pkg/errors"
)

type DataRows struct {
	TabRowDelimiter string        `xml:"tabrowDelimiter,attr,omitempty"`
	Columns         DelimitedList `xml:"columns,attr,omitempty"`
	Rows            []*DataRow    `xml:"row"`
	TabRows         []string      `xml:"tabrow"`
}

type DataRow struct {
	Columns []*DataCol `xml:"col"`
	Delete  bool       `xml:"delete,attr,omitempty"` // TODO(go,core) does this un/marshal properly?
}

type DataCol struct {
	Null  bool   `xml:"null,attr,omitempty"`
	Empty bool   `xml:"empty,attr,omitempty"`
	Sql   bool   `xml:"sql,attr,omitempty"`
	Text  string `xml:",chardata"`
}

func (self *DataRows) AddColumn(name string, value string) error {
	if self.HasColumn(name) {
		return errors.Errorf("already has column %s", name)
	}
	self.Columns = append(self.Columns, name)
	for _, row := range self.Rows {
		// TODO(feat) what about nulls?
		row.Columns = append(row.Columns, &DataCol{Text: value})
	}
	return nil
}

func (self *DataRows) HasColumn(name string) bool {
	for _, col := range self.Columns {
		if col == name {
			return true
		}
	}
	return false
}

// Replaces TabRows with Rows
func (self *DataRows) ConvertTabRows() {
	delimiter := util.CoalesceStr(self.TabRowDelimiter, "\t")
	delimiter = strings.ReplaceAll(delimiter, "\\t", "\t")
	delimiter = strings.ReplaceAll(delimiter, "\\n", "\n")
	self.TabRowDelimiter = ""

	for _, tabrow := range self.TabRows {
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
	self.TabRows = nil
}

func (self *DataRows) GetColMap(row *DataRow) map[string]*DataCol {
	return self.GetColMapKeys(row, self.Columns)
}

func (self *DataRows) GetColMapKeys(row *DataRow, keys []string) map[string]*DataCol {
	out := map[string]*DataCol{}
	for i, col := range row.Columns {
		if util.IIndexOfStr(self.Columns[i], keys) >= 0 {
			out[self.Columns[i]] = col
		}
	}
	return out
}

func (self *DataRows) TryGetRowMatchingColMap(colmap map[string]*DataCol) *DataRow {
	for _, row := range self.Rows {
		if self.RowMatchesColMap(row, colmap) {
			return row
		}
	}
	return nil
}

// `row` matches `colmap` if all the columns in colmap match a column in the row
func (self *DataRows) RowMatchesColMap(row *DataRow, colmap map[string]*DataCol) bool {
	for colName, col := range colmap {
		// find the corresponding column
		idx := util.IIndexOfStr(colName, self.Columns)
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

func (self *DataRows) tryGetColIndexesOfNames(names []string) ([]int, bool) {
	out := make([]int, len(names))
	for i, name := range names {
		found := false
		for j, col := range self.Columns {
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
func (self *DataRows) RowEquals(ownRow, otherRow *DataRow, otherColumns []string) bool {
	if len(self.Columns) != len(otherColumns) {
		return false
	}

	if ownRow.Delete != otherRow.Delete {
		return false
	}

	otherIndexes, ok := self.tryGetColIndexesOfNames(otherColumns)
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

func (self *DataCol) Equals(other *DataCol) bool {
	if self == nil || other == nil {
		return false
	}
	if self.Null && other.Null {
		return true
	}
	if self.Empty && other.Empty {
		return true
	}
	if self.Sql != other.Sql {
		return false
	}
	// TODO(feat) something other than plain old string equality?
	return self.Text == other.Text
}
