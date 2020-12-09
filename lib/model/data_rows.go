package model

import "github.com/pkg/errors"

type DataRows struct {
	Columns DelimitedList `xml:"columns,attr"`
	Rows    []*DataRow    `xml:"row"`
}

func (self *DataRows) AddColumn(name string, value string) error {
	if self.HasColumn(name) {
		return errors.Errorf("already has column %s", name)
	}
	self.Columns = append(self.Columns, name)
	for _, row := range self.Rows {
		row.Columns = append(row.Columns, value)
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

type DataRow struct {
	Columns []string `xml:"col"`
	Delete  bool     `xml:"delete,attr"` // TODO(go,core) does this un/marshal properly?
}
