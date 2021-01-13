package model

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
)

type FuncParamDir string

const (
	FuncParamDirIn    FuncParamDir = "IN"
	FuncParamDirOut   FuncParamDir = "OUT"
	FuncParamDirInOut FuncParamDir = "INOUT"
)

type Function struct {
	Name            string                `xml:"name,attr,omitempty"`
	Owner           string                `xml:"owner,attr,omitempty"`
	Description     string                `xml:"description,attr,omitempty"`
	Returns         string                `xml:"returns,attr,omitempty"`
	CachePolicy     string                `xml:"cachePolicy,attr,omitempty"`
	SecurityDefiner bool                  `xml:"securityDefiner,attr,omitempty"`
	SlonySetId      *int                  `xml:"slonySetId,attr,omitempty"`
	Parameters      []*FunctionParameter  `xml:"functionParameter"`
	Definitions     []*FunctionDefinition `xml:"functionDefinition"`
	Grants          []*Grant              `xml:"grant"`
}

type FunctionParameter struct {
	Name      string       `xml:"name,attr,omitempty"`
	Type      string       `xml:"type,attr"`
	Direction FuncParamDir `xml:"direction,attr,omitempty"`
}

type FunctionDefinition struct {
	SqlFormat SqlFormat `xml:"sqlFormat,attr,omitempty"`
	Language  string    `xml:"language,attr,omitempty"`
	Text      string    `xml:",cdata"`
}

func (self *Function) HasDefinition(sqlFormat SqlFormat) bool {
	return self.TryGetDefinition(sqlFormat) != nil
}

func (self *Function) TryGetDefinition(sqlFormat SqlFormat) *FunctionDefinition {
	for _, def := range self.Definitions {
		if def.SqlFormat.Equals(sqlFormat) {
			return def
		}
	}
	return nil
}

func (self *Function) AddParameter(name, datatype string) {
	// TODO(feat) sanity check
	self.Parameters = append(self.Parameters, &FunctionParameter{
		Name: name,
		Type: datatype,
	})
}

func (self *Function) ParamTypes() []string {
	out := make([]string, len(self.Parameters))
	for i, param := range self.Parameters {
		out[i] = param.Type
	}
	return out
}

func (self *Function) ParamSigs() []string {
	out := make([]string, len(self.Parameters))
	for i, param := range self.Parameters {
		out[i] = util.CondJoin(" ", string(param.Direction), param.Name, param.Type)
	}
	return out
}

func (self *Function) GetGrants() []*Grant {
	return self.Grants
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
}

func (self *FunctionParameter) IdentityMatches(other *FunctionParameter) bool {
	// TODO(feat) more robust type identity checking.
	// e.g. does postgres consider text and varchar parameters to be equal?
	return strings.EqualFold(self.Name, other.Name) && strings.EqualFold(self.Type, other.Type)
}
