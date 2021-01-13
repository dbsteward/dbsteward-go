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

// attempt to find a row in `self.Rows` which has the same values of `key` columns as `target`
func (self *DataRows) TryGetRowMatchingKeyCols(target *DataRow, key []string) *DataRow {
	indexes, ok := self.tryGetColIndexesOfNames(key)
	if !ok {
		return nil
	}

outer:
	for _, row := range self.Rows {
		for _, index := range indexes {
			if !target.Columns[index].Equals(row.Columns[index]) {
				continue outer
			}
		}
		return row
	}
	return nil
}

// get the columns of the `target` row for the matching columns given by `key`
func (self *DataRows) TryGetColsMatchingKeyCols(target *DataRow, key []string) ([]*DataCol, bool) {
	colIndexes, ok := self.tryGetColIndexesOfNames(key)
	if !ok {
		return nil, false
	}
	out := make([]*DataCol, len(key))
	for i, idx := range colIndexes {
		out[i] = target.Columns[idx]
	}
	return out, true
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
