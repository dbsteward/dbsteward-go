package xml

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
)

type Function struct {
	Name            string                `xml:"name,attr"`
	Owner           string                `xml:"owner,attr,omitempty"`
	Description     string                `xml:"description,attr,omitempty"`
	Returns         string                `xml:"returns,attr"`
	CachePolicy     string                `xml:"cachePolicy,attr,omitempty"`
	ForceRedefine   bool                  `xml:"forceRedefine,attr,omitempty"`
	SecurityDefiner bool                  `xml:"securityDefiner,attr,omitempty"`
	SlonySetId      *int                  `xml:"slonySetId,attr,omitempty"`
	Parameters      []*FunctionParameter  `xml:"functionParameter"`
	Definitions     []*FunctionDefinition `xml:"functionDefinition"`
	Grants          []*Grant              `xml:"grant"`
}

type FunctionParameter struct {
	Name      string `xml:"name,attr,omitempty"`
	Type      string `xml:"type,attr"`
	Direction string `xml:"direction,attr,omitempty"`
}

type FunctionDefinition struct {
	SqlFormat string `xml:"sqlFormat,attr,omitempty"`
	Language  string `xml:"language,attr,omitempty"`
	Text      string `xml:",cdata"`
}

func (self *Function) HasDefinition(sqlFormat string) bool {
	return self.TryGetDefinition(sqlFormat).HasValue()
}

func (self *Function) TryGetDefinition(sqlFormat string) util.Opt[*FunctionDefinition] {
	return util.Find(self.Definitions, func(def *FunctionDefinition) bool {
		return strings.EqualFold(def.SqlFormat, sqlFormat)
	})
}

func (self *Function) AddParameter(name, datatype string) {
	// TODO(feat) sanity check
	self.Parameters = append(self.Parameters, &FunctionParameter{
		Name: name,
		Type: datatype,
	})
}

func (self *Function) AddGrant(grant *Grant) {
	// TODO(feat) sanity check
	self.Grants = append(self.Grants, grant)
}

func (self *Function) IdentityMatches(other *Function) bool {
	if other == nil {
		return false
	}
	if !strings.EqualFold(self.Name, other.Name) {
		return false
	}
	if len(self.Parameters) != len(other.Parameters) {
		return false
	}
	for i, param := range self.Parameters {
		if !param.IdentityMatches(other.Parameters[i]) {
			return false
		}
	}
	// only return true if both functions have a definition that match
	for _, selfDef := range self.Definitions {
		for _, otherDef := range other.Definitions {
			if selfDef.IdentityMatches(otherDef) {
				return true
			}
		}
	}
	return false
}

func (self *Function) Equals(other *Function, sqlFormat string) bool {
	if self == nil || other == nil {
		return false
	}

	if !self.IdentityMatches(other) {
		return false
	}

	// NOTE: old dbsteward uses xml_parser::role_enum but as far as I can tell that's homomorphic?
	if self.Owner != other.Owner {
		return false
	}

	// TODO(feat) what about no-op changes like "character varying" => "varchar"
	if self.Returns != other.Returns {
		return false
	}

	selfDef := self.TryGetDefinition(sqlFormat)
	otherDef := self.TryGetDefinition(sqlFormat)
	return selfDef.Equals(otherDef)
}

func (self *Function) Merge(overlay *Function) {
	self.Owner = overlay.Owner
	self.Description = overlay.Description
	self.Returns = overlay.Returns
	self.CachePolicy = overlay.CachePolicy
	// don't bother to merge parameters or definitions, just replace them wholesale
	self.Parameters = overlay.Parameters
	self.Definitions = overlay.Definitions

	for _, grant := range overlay.Grants {
		self.AddGrant(grant)
	}
}

func (self *FunctionParameter) IdentityMatches(other *FunctionParameter) bool {
	if self == nil || other == nil {
		return false
	}
	// TODO(feat) more robust type identity checking.
	// e.g. does postgres consider text and varchar parameters to be equal? do parameter names matter?
	return strings.EqualFold(self.Name, other.Name) &&
		strings.EqualFold(self.Type, other.Type) &&
		strings.EqualFold(self.Direction, other.Direction)
}

func (self *FunctionDefinition) IdentityMatches(other *FunctionDefinition) bool {
	return strings.EqualFold(self.SqlFormat, other.SqlFormat)
}

func (self *FunctionDefinition) Equals(other *FunctionDefinition) bool {
	if self == nil || other == nil {
		return false
	}

	// TODO(go,core) old dbsteward conditionally ignores whitespace changes per sqlformat. is that necessary?
	return strings.EqualFold(self.SqlFormat, other.SqlFormat) &&
		strings.EqualFold(self.Language, other.Language) &&
		self.Text == other.Text
}
