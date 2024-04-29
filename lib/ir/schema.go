package ir

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/pkg/errors"
)

type Schema struct {
	Name        string
	Description string
	Owner       string
	SlonySetId  util.Opt[int]
	Tables      []*Table
	Grants      []*Grant
	Types       []*TypeDef
	Sequences   []*Sequence
	Functions   []*Function
	Triggers    []*Trigger
	Views       []*View
}

// TODO(go,4) triggers are schema objects, but always only in the scope of a single table. consider moving it to Table

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
	if self == nil {
		return nil
	}
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

func (self *Schema) TryGetTypeNamed(name string) *TypeDef {
	if self == nil {
		return nil
	}
	for _, t := range self.Types {
		// TODO(feat) case insensitivity?
		if t.Name == name {
			return t
		}
	}
	return nil
}

func (self *Schema) AddType(t *TypeDef) {
	// TODO(feat) sanity check
	self.Types = append(self.Types, t)
}

func (self *Schema) TryGetSequenceNamed(name string) *Sequence {
	if self == nil {
		return nil
	}
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
	if self == nil {
		return nil
	}
	for _, view := range self.Views {
		// TODO(feat) case insensitivity?
		if view.Name == name {
			return view
		}
	}
	return nil
}

func (self *Schema) TryGetRelationNamed(name string) Relation {
	if self == nil {
		return nil
	}
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
	if self == nil {
		return nil
	}
	for _, function := range self.Functions {
		// TODO(go,core) should we return the matched definition?
		match, _ := function.IdentityMatches(target)
		if match {
			return function
		}
	}
	return nil
}

func (self *Schema) AddFunction(function *Function) {
	// TODO(feat) sanity check
	self.Functions = append(self.Functions, function)
}

func (self *Schema) GetTriggersForTableNamed(table string) []*Trigger {
	if self == nil {
		return nil
	}
	out := []*Trigger{}
	for _, trigger := range self.Triggers {
		if strings.EqualFold(trigger.Table, table) {
			out = append(out, trigger)
		}
	}
	return out
}

func (self *Schema) TryGetTriggerNamedForTable(name, table string) *Trigger {
	if self == nil {
		return nil
	}
	for _, trigger := range self.Triggers {
		if trigger.Name == name && trigger.Table == table {
			return trigger
		}
	}
	return nil
}

func (self *Schema) TryGetTriggerMatching(target *Trigger) *Trigger {
	if self == nil {
		return nil
	}
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

func (self *Schema) GetGrants() []*Grant {
	return self.Grants
}

func (self *Schema) AddGrant(grant *Grant) {
	// TODO(feat) sanity check
	self.Grants = append(self.Grants, grant)
}

// TODO(go,nth) replace other schema name matches with IdentityMatches where possible
// TODO(go,nth) replace doc.TryGetSchemaNamed with TryGetSchemaMatching where possible
func (self *Schema) IdentityMatches(other *Schema) bool {
	if self == nil || other == nil {
		return false
	}
	// TODO(feat) case sensitivity based on engine+quotedness
	return strings.EqualFold(self.Name, other.Name)
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

func (self *Schema) Validate(doc *Definition) []error {
	// TODO(go,3) check owner, remove from other codepaths
	// TODO(go,nth) validate grants
	out := []error{}

	// no two objects should have same identity (also, validate sub-objects)
	for i, table := range self.Tables {
		out = append(out, table.Validate(doc, self)...)
		for _, other := range self.Tables[i+1:] {
			if table.IdentityMatches(other) {
				out = append(out, fmt.Errorf("found two tables in schema %s with name %q", self.Name, table.Name))
			}
		}
	}
	for i, datatype := range self.Types {
		out = append(out, datatype.Validate(doc, self)...)
		for _, other := range self.Types[i+1:] {
			if datatype.IdentityMatches(other) {
				out = append(out, fmt.Errorf("found two types in schema %s with name %q", self.Name, datatype.Name))
			}
		}
	}
	for i, sequence := range self.Sequences {
		out = append(out, sequence.Validate(doc, self)...)
		for _, other := range self.Sequences[i+1:] {
			if sequence.IdentityMatches(other) {
				out = append(out, fmt.Errorf("found two sequences in schema %s with name %q", self.Name, sequence.Name))
			}
		}
	}
	for i, function := range self.Functions {
		out = append(out, function.Validate(doc, self)...)
		for _, other := range self.Functions[i+1:] {
			match, def := function.IdentityMatches(other)
			if match {
				out = append(out, fmt.Errorf(
					"found two functions in schema %s with signature %s for sql format %s",
					self.Name, function.ShortSig(), def.SqlFormat,
				))
			}
		}
	}
	for i, trigger := range self.Triggers {
		out = append(out, trigger.Validate(doc, self)...)
		for _, other := range self.Triggers[i+1:] {
			if trigger.IdentityMatches(other) {
				out = append(out, fmt.Errorf("found two triggers in schema %s with name %q", self.Name, trigger.Name))
			}
		}
	}
	for i, view := range self.Views {
		out = append(out, view.Validate(doc, self)...)
		for _, other := range self.Views[i+1:] {
			if view.IdentityMatches(other) {
				out = append(out, fmt.Errorf("found two views in schema %s with name %q", self.Name, view.Name))
			}
		}
	}

	return out
}
