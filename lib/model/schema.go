package model

import "github.com/pkg/errors"

type Schema struct {
	Name      string      `xml:"name,attr"`
	Tables    []*Table    `xml:"table"`
	Grants    []*Grant    `xml:"grant"`
	Types     []*DataType `xml:"type"`
	Sequences []*Sequence `xml:"sequence"`
	Functions []*Function `xml:"function"`
	Triggers  []*Trigger  `xml:"trigger"`
	Views     []*View     `xml:"view"`
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
