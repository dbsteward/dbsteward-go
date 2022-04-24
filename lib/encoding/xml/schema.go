package xml

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
)

type Schema struct {
	Name        string      `xml:"name,attr"`
	Description string      `xml:"description,attr,omitempty"`
	Owner       string      `xml:"owner,attr,omitempty"`
	SlonySetId  *int        `xml:"slonySetId,attr,omitempty"`
	Tables      []*Table    `xml:"table"`
	Grants      []*Grant    `xml:"grant"`
	Types       []*DataType `xml:"type"`
	Sequences   []*Sequence `xml:"sequence"`
	Functions   []*Function `xml:"function"`
	Triggers    []*Trigger  `xml:"trigger"`
	Views       []*View     `xml:"view"`
}

// func (self *Schema) GetTableNamed(name string) (*Table, error) {
// 	matching := []*Table{}
// 	for _, table := range self.Tables {
// 		if table.Name == name {
// 			matching = append(matching, table)
// 		}
// 	}
// 	if len(matching) == 0 {
// 		return nil, errors.Errorf("no table named %s found", name)
// 	}
// 	if len(matching) > 1 {
// 		return nil, errors.Errorf("more than one table named %s found", name)
// 	}
// 	return matching[0], nil
// }

// func (self *Schema) TryGetTableNamed(name string) *Table {
// 	if self == nil {
// 		return nil
// 	}
// 	for _, table := range self.Tables {
// 		// TODO(feat) case insensitivity?
// 		if table.Name == name {
// 			return table
// 		}
// 	}
// 	return nil
// }

func (self *Schema) AddTable(table *Table) {
	// TODO(feat) sanity check
	self.Tables = append(self.Tables, table)
}

// func (self *Schema) TryGetTypeNamed(name string) *DataType {
// 	if self == nil {
// 		return nil
// 	}
// 	for _, t := range self.Types {
// 		// TODO(feat) case insensitivity?
// 		if t.Name == name {
// 			return t
// 		}
// 	}
// 	return nil
// }

func (self *Schema) AddType(t *DataType) {
	// TODO(feat) sanity check
	self.Types = append(self.Types, t)
}

// func (self *Schema) TryGetSequenceNamed(name string) *Sequence {
// 	if self == nil {
// 		return nil
// 	}
// 	for _, sequence := range self.Sequences {
// 		// TODO(feat) case insensitivity?
// 		if sequence.Name == name {
// 			return sequence
// 		}
// 	}
// 	return nil
// }

func (self *Schema) AddSequence(sequence *Sequence) {
	// TODO(feat) sanity check
	self.Sequences = append(self.Sequences, sequence)
}

// func (self *Schema) TryGetViewNamed(name string) *View {
// 	if self == nil {
// 		return nil
// 	}
// 	for _, view := range self.Views {
// 		// TODO(feat) case insensitivity?
// 		if view.Name == name {
// 			return view
// 		}
// 	}
// 	return nil
// }

// func (self *Schema) TryGetRelationNamed(name string) Relation {
// 	if self == nil {
// 		return nil
// 	}
// 	table := self.TryGetTableNamed(name)
// 	if table != nil {
// 		return table
// 	}
// 	return self.TryGetViewNamed(name)
// }

func (self *Schema) AddView(view *View) {
	// TODO(feat) sanity check
	self.Views = append(self.Views, view)
}

// func (self *Schema) TryGetFunctionMatching(target *Function) *Function {
// 	if self == nil {
// 		return nil
// 	}
// 	for _, function := range self.Functions {
// 		// TODO(go,core) should we return the matched definition?
// 		match, _ := function.IdentityMatches(target)
// 		if match {
// 			return function
// 		}
// 	}
// 	return nil
// }

func (self *Schema) AddFunction(function *Function) {
	// TODO(feat) sanity check
	self.Functions = append(self.Functions, function)
}

// func (self *Schema) GetTriggersForTableNamed(table string) []*Trigger {
// 	if self == nil {
// 		return nil
// 	}
// 	out := []*Trigger{}
// 	for _, trigger := range self.Triggers {
// 		if strings.EqualFold(trigger.Table, table) {
// 			out = append(out, trigger)
// 		}
// 	}
// 	return out
// }

// func (self *Schema) TryGetTriggerNamedForTable(name, table string) *Trigger {
// 	if self == nil {
// 		return nil
// 	}
// 	for _, trigger := range self.Triggers {
// 		if trigger.Name == name && trigger.Table == table {
// 			return trigger
// 		}
// 	}
// 	return nil
// }

// func (self *Schema) TryGetTriggerMatching(target *Trigger) *Trigger {
// 	if self == nil {
// 		return nil
// 	}
// 	for _, trigger := range self.Triggers {
// 		if trigger.IdentityMatches(target) {
// 			return trigger
// 		}
// 	}
// 	return nil
// }

func (self *Schema) AddTrigger(trigger *Trigger) {
	// TODO(feat) sanity check
	self.Triggers = append(self.Triggers, trigger)
}

func (self *Schema) GetGrants() []*Grant {
	return self.Grants
}

func (self *Schema) AddGrant(grant *Grant) {
	// TODO(feat) sanity check
	self.Grants = append(self.Grants, grant)
}

func (self *Schema) IdentityMatches(other *Schema) bool {
	if self == nil || other == nil {
		return false
	}
	// TODO(feat) case sensitivity based on engine+quotedness
	return strings.EqualFold(self.Name, other.Name)
}

func (base *Schema) Merge(overlay *Schema) {
	if overlay == nil {
		return
	}

	base.Description = overlay.Description
	base.Owner = overlay.Owner
	base.SlonySetId = overlay.SlonySetId

	for _, overlayTable := range overlay.Tables {
		if baseTable, ok := util.FindMatching(base.Tables, overlayTable).Maybe(); ok {
			baseTable.Merge(overlayTable)
		} else {
			base.AddTable(overlayTable)
		}
	}

	// grants are always appended, not overwritten
	for _, overlayGrant := range overlay.Grants {
		base.AddGrant(overlayGrant)
	}

	for _, overlayType := range overlay.Types {
		if baseType, ok := util.FindMatching(base.Types, overlayType).Maybe(); ok {
			baseType.Merge(overlayType)
		} else {
			base.AddType(overlayType)
		}
	}

	for _, overlaySeq := range overlay.Sequences {
		if baseSeq, ok := util.FindMatching(base.Sequences, overlaySeq).Maybe(); ok {
			baseSeq.Merge(overlaySeq)
		} else {
			base.AddSequence(overlaySeq)
		}
	}

	for _, overlayFunc := range overlay.Functions {
		if baseFunc, ok := util.FindMatching(base.Functions, overlayFunc).Maybe(); ok {
			baseFunc.Merge(overlayFunc)
		} else {
			base.AddFunction(overlayFunc)
		}
	}

	for _, overlayTrigger := range overlay.Triggers {
		if baseTrigger, ok := util.FindMatching(base.Triggers, overlayTrigger).Maybe(); ok {
			baseTrigger.Merge(overlayTrigger)
		} else {
			base.AddTrigger(overlayTrigger)
		}
	}

	for _, overlayView := range overlay.Views {
		if baseView, ok := util.FindMatching(base.Views, overlayView).Maybe(); ok {
			baseView.Merge(overlayView)
		} else {
			base.AddView(overlayView)
		}
	}
}
