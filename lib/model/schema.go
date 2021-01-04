package model

import "github.com/pkg/errors"

type Schema struct {
	Name        string      `xml:"name,attr"`
	Description string      `xml:"description,attr"`
	Owner       string      `xml:"owner,attr"`
	SlonySetId  int         `xml:"slonySetId,attr"`
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

func (self *Schema) TryGetTypeNamed(name string) *DataType {
	for _, t := range self.Types {
		// TODO(feat) case insensitivity?
		if t.Name == name {
			return t
		}
	}
	return nil
}

func (self *Schema) AddType(t *DataType) {
	// TODO(feat) sanity check
	self.Types = append(self.Types, t)
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

func (self *Schema) TryGetFunctionMatching(target *Function) *Function {
	for _, function := range self.Functions {
		if function.IdentityMatches(target) {
			return function
		}
	}
	return nil
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

func (self *Schema) TryGetTriggerMatching(target *Trigger) *Trigger {
	for _, trigger := range self.Triggers {
		if trigger.IdentityMatches(target) {
			return trigger
		}
	}
	return nil
}

func (self *Schema) AddTrigger(trigger *Trigger) {
	// TODO(feat) sanity check
	self.Triggers = append(self.Triggers, trigger)
}

func (self *Schema) AddGrant(grant *Grant) {
	// TODO(feat) sanity check
	self.Grants = append(self.Grants, grant)
}

func (self *Schema) Merge(overlay *Schema) {
	if overlay == nil {
		return
	}

	self.Description = overlay.Description
	self.Owner = overlay.Owner
	self.SlonySetId = overlay.SlonySetId

	for _, overlayTable := range overlay.Tables {
		if baseTable := self.TryGetTableNamed(overlayTable.Name); baseTable != nil {
			baseTable.Merge(overlayTable)
		} else {
			self.AddTable(overlayTable)
		}
	}

	// grants are always appended, not overwritten
	for _, overlayGrant := range overlay.Grants {
		self.AddGrant(overlayGrant)
	}

	for _, overlayType := range overlay.Types {
		if baseType := self.TryGetTypeNamed(overlayType.Name); baseType != nil {
			baseType.Merge(overlayType)
		} else {
			self.AddType(overlayType)
		}
	}

	for _, overlaySeq := range overlay.Sequences {
		if baseSeq := self.TryGetSequenceNamed(overlaySeq.Name); baseSeq != nil {
			baseSeq.Merge(overlaySeq)
		} else {
			self.AddSequence(overlaySeq)
		}
	}

	for _, overlayFunc := range overlay.Functions {
		if baseFunc := self.TryGetFunctionMatching(overlayFunc); baseFunc != nil {
			baseFunc.Merge(overlayFunc)
		} else {
			self.AddFunction(overlayFunc)
		}
	}

	for _, overlayTrig := range overlay.Triggers {
		if baseTrig := self.TryGetTriggerMatching(overlayTrig); baseTrig != nil {
			baseTrig.Merge(overlayTrig)
		} else {
			self.AddTrigger(overlayTrig)
		}
	}

	for _, overlayView := range overlay.Views {
		if baseView := self.TryGetViewNamed(overlayView.Name); baseView != nil {
			baseView.Merge(overlayView)
		} else {
			self.AddView(overlayView)
		}
	}
}
