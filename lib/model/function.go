package model

import (
	"strings"
)

// TODO(go, core) finish fleshing this out

type Function struct {
	Name        string                `xml:"name,attr"`
	Owner       string                `xml:"owner,attr"`
	Description string                `xml:"description,attr"`
	Returns     string                `xml:"returns,attr"`
	CachePolicy string                `xml:"cachePolicy,attr"`
	Parameters  []*FunctionParameter  `xml:"functionParameter"`
	Definitions []*FunctionDefinition `xml:"functionDefinition"`
	Grants      []*Grant              `xml:"grant"`
	Revokes     []*Revoke             `xml:"revoke"`
}

type FunctionParameter struct {
	Name string `xml:"name,attr"`
	Type string `xml:"type,attr"`
}

type FunctionDefinition struct {
	SqlFormat SqlFormat `xml:"sqlFormat"`
	Language  string    `xml:"language"`
	Text      string    `xml:",chardata"`
}

func (self *Function) HasDefinition() bool {
	// TODO(go,core)
	return false
}

func (self *Function) TryGetDefinition() (*FunctionDefinition, bool) {
	// TODO(go,core) see pgsql8_function::has_definition and get_definition
	return nil, false
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

func (self *Function) AddRevoke(revoke *Revoke) {
	// TODO(feat) sanity check
	self.Revokes = append(self.Revokes, revoke)
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
	// only return true if both functions have a definition for the same sql format
	for _, selfDef := range self.Definitions {
		for _, otherDef := range other.Definitions {
			if selfDef.SqlFormat.Equals(otherDef.SqlFormat) {
				return true
			}
		}
	}
	return false
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
	for _, revoke := range overlay.Revokes {
		self.AddRevoke(revoke)
	}
}

func (self *FunctionParameter) IdentityMatches(other *FunctionParameter) bool {
	// TODO(feat) more robust type identity checking.
	// e.g. does postgres consider text and varchar parameters to be equal?
	return strings.EqualFold(self.Name, other.Name) && strings.EqualFold(self.Type, other.Type)
}
