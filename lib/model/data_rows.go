package model

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/pkg/errors"
)

type DataRows struct {
	TabRowDelimiter string        `xml:"tabrowDelimiter,attr"`
	Columns         DelimitedList `xml:"columns,attr"`
	Rows            []*DataRow    `xml:"row"`
	TabRows         []string      `xml:"tabrow"`
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

type DataRow struct {
	Columns []*DataCol `xml:"col"`
	Delete  bool       `xml:"delete,attr"` // TODO(go,core) does this un/marshal properly?
}

type DataCol struct {
	Null bool   `xml:"null,attr"`
	Text string `xml:",chardata"`
}
