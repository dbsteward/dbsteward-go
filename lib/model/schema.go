package model

import "github.com/pkg/errors"

type Schema struct {
	Name   string   `xml:"name,attr"`
	Tables []*Table `xml:"table"`
}

func (self *Schema) GetTableNamed(name string) (*Table, error) {
	matching := []*Table{}
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
