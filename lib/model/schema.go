package model

import "github.com/pkg/errors"

type Schema struct {
	Name        string      `xml:"name,attr"`
	Description string      `xml:"description,attr"`
	Owner       string      `xml:"owner,attr"`
	Tables      []*Table    `xml:"table"`
	Grants      []*Grant    `xml:"grant"`
	Types       []*DataType `xml:"type"`
	Sequences   []*Sequence `xml:"sequence"`
	Functions   []*Function `xml:"function"`
	Triggers    []*Trigger  `xml:"trigger"`
	Views       []*View     `xml:"view"`
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

func (self *Schema) TryGetTableNamed(name string) *Table {
	for _, table := range self.Tables {
		// TODO(feat) case insensitivity?
		if table.Name == name {
			return table
		}
	}
	return nil
}

func (self *Schema) AddTable(table *Table) {
	// TODO(feat) sanity check
	self.Tables = append(self.Tables, table)
}

func (self *Schema) TryGetSequenceNamed(name string) *Sequence {
	for _, sequence := range self.Sequences {
		// TODO(feat) case insensitivity?
		if sequence.Name == name {
			return sequence
		}
	}
	return nil
}

func (self *Schema) AddSequence(sequence *Sequence) {
	// TODO(feat) sanity check
	self.Sequences = append(self.Sequences, sequence)
}

func (self *Schema) TryGetViewNamed(name string) *View {
	for _, view := range self.Views {
		// TODO(feat) case insensitivity?
		if view.Name == name {
			return view
		}
	}
	return nil
}

func (self *Schema) TryGetRelationNamed(name string) Relation {
	table := self.TryGetTableNamed(name)
	if table != nil {
		return table
	}
	return self.TryGetViewNamed(name)
}

func (self *Schema) AddView(view *View) {
	// TODO(feat) sanity check
	self.Views = append(self.Views, view)
}

func (self *Schema) AddFunction(function *Function) {
	// TODO(feat) sanity check
	self.Functions = append(self.Functions, function)
}

func (self *Schema) TryGetTriggerNamedForTable(name, table string) *Trigger {
	for _, trigger := range self.Triggers {
		if trigger.Name == name && trigger.Table == table {
			return trigger
		}
	}
	return nil
}

func (self *Schema) AddTrigger(trigger *Trigger) {
	// TODO(feat) sanity check
	self.Triggers = append(self.Triggers, trigger)
}
