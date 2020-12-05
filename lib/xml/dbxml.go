package xml

import (
	"encoding/xml"
	"os"

	"github.com/pkg/errors"
)

func LoadDbXmlFile(file string) (*DbDocument, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read dbxml file %s", file)
	}
	defer f.Close()

	doc := &DbDocument{}
	err = xml.NewDecoder(f).Decode(doc)
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse dbxml file %s", file)
	}
	return doc, nil
}

type DbDocument struct {
	Schemas []*DbSchema `xml:"schema"`
}

func (self *DbDocument) GetSchemaNamed(name string) (*DbSchema, error) {
	matching := []*DbSchema{}
	for _, schema := range self.Schemas {
		if schema.Name == name {
			matching = append(matching, schema)
		}
	}
	if len(matching) == 0 {
		return nil, errors.Errorf("no schema named %s found", name)
	}
	if len(matching) > 1 {
		return nil, errors.Errorf("more than one schema named %s found", name)
	}
	return matching[0], nil
}

type DbSchema struct {
	Name   string     `xml:"name,attr"`
	Tables []*DbTable `xml:"table"`
}

func (self *DbSchema) GetTableNamed(name string) (*DbTable, error) {
	matching := []*DbTable{}
	for _, table := range self.Tables {
		if table.Name == name {
			matching = append(matching, table)
		}
	}
	if len(matching) == 0 {
		return nil, errors.Errorf("no table named %s found", name)
	}
	if len(matching) > 1 {
		return nil, errors.Errorf("more than one table named %s found", name)
	}
	return matching[0], nil
}

type DbTable struct {
	Name string  `xml:"name,attr"`
	Rows *DbRows `xml:"rows"`
}

type DbRows struct {
	Columns DelimitedList `xml:"columns,attr"`
	Rows    []*DbRow      `xml:"row"`
}

func (self *DbRows) AddColumn(name string, value string) error {
	if self.HasColumn(name) {
		return errors.Errorf("already has column %s", name)
	}
	self.Columns = append(self.Columns, name)
	for _, row := range self.Rows {
		row.Columns = append(row.Columns, value)
	}
	return nil
}

func (self *DbRows) HasColumn(name string) bool {
	for _, col := range self.Columns {
		if col == name {
			return true
		}
	}
	return false
}

type DbRow struct {
	Columns []string `xml:"col"`
}
