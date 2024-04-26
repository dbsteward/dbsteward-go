package ir

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
)

type FuncParamDir string

const (
	FuncParamDirIn    FuncParamDir = "IN"
	FuncParamDirOut   FuncParamDir = "OUT"
	FuncParamDirInOut FuncParamDir = "INOUT"
)

func NewFuncParamDir(s string) (FuncParamDir, error) {
	if s == "" {
		return FuncParamDirIn, nil
	}
	v := FuncParamDir(s)
	if v.Equals(FuncParamDirIn) {
		return FuncParamDirIn, nil
	}
	if v.Equals(FuncParamDirOut) {
		return FuncParamDirOut, nil
	}
	if v.Equals(FuncParamDirInOut) {
		return FuncParamDirInOut, nil
	}
	return FuncParamDirIn, fmt.Errorf("invalid function parameter direction '%s'", s)
}

func (self FuncParamDir) Equals(other FuncParamDir) bool {
	return strings.EqualFold(string(self), string(other))
}

type Function struct {
	Name            string
	Owner           string
	Description     string
	Returns         string
	CachePolicy     string
	ForceRedefine   bool
	SecurityDefiner bool
	Parameters      []*FunctionParameter
	Definitions     []*FunctionDefinition
	Grants          []*Grant
}

type FunctionParameter struct {
	Name      string
	Type      string
	Direction FuncParamDir
}

type FunctionDefinition struct {
	SqlFormat SqlFormat
	Language  string
	Text      string
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

func (self *Function) AddParameter(name, datatype string, direction FuncParamDir) {
	// TODO(feat) sanity check
	self.Parameters = append(self.Parameters, &FunctionParameter{
		Name:      name,
		Type:      datatype,
		Direction: direction,
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

func (self *Function) ShortSig() string {
	return fmt.Sprintf("%s(%s)", self.Name, strings.Join(self.ParamTypes(), ", "))
}

func (self *Function) GetGrants() []*Grant {
	return self.Grants
}

func (self *Function) AddGrant(grant *Grant) {
	// TODO(feat) sanity check
	self.Grants = append(self.Grants, grant)
}

func (self *Function) IdentityMatches(other *Function) (bool, *FunctionDefinition) {
	if other == nil {
		return false, nil
	}
	if !strings.EqualFold(self.Name, other.Name) {
		return false, nil
	}
	if len(self.Parameters) != len(other.Parameters) {
		return false, nil
	}
	for i, param := range self.Parameters {
		if !param.IdentityMatches(other.Parameters[i]) {
			return false, nil
		}
	}
	// only return true if both functions have a definition that match
	for _, selfDef := range self.Definitions {
		for _, otherDef := range other.Definitions {
			if selfDef.IdentityMatches(otherDef) {
				return true, selfDef
			}
		}
	}
	return false, nil
}

func (self *Function) Equals(other *Function, sqlFormat SqlFormat) bool {
	if self == nil || other == nil {
		return false
	}

	// TODO(go,core) should we consider identity part of equality?
	match, _ := self.IdentityMatches(other)
	if !match {
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

func (self *Function) Validate(doc *Definition, schema *Schema) []error {
	// TODO(go,3) validate owner, remove from other codepaths
	// TODO(go,3) validate parameters
	out := []error{}
	for i, def := range self.Definitions {
		out = append(out, def.Validate(doc, schema, self)...)
		for _, other := range self.Definitions[i+1:] {
			if def.IdentityMatches(other) {
				out = append(out, fmt.Errorf(
					"found two definitions for %s.%s for sql format %s",
					schema.Name, self.ShortSig(), def.SqlFormat,
				))
			}
		}
	}
	return out
}

func (self *FunctionParameter) IdentityMatches(other *FunctionParameter) bool {
	if self == nil || other == nil {
		return false
	}
	// TODO(feat) more robust type identity checking.
	// e.g. does postgres consider text and varchar parameters to be equal? do parameter names matter?
	return strings.EqualFold(self.Name, other.Name) &&
		strings.EqualFold(self.Type, other.Type) &&
		self.Direction.Equals(other.Direction)
}

func (self *FunctionDefinition) IdentityMatches(other *FunctionDefinition) bool {
	return self.SqlFormat.Equals(other.SqlFormat)
}

func (self *FunctionDefinition) Equals(other *FunctionDefinition) bool {
	if self == nil || other == nil {
		return false
	}

	// TODO(go,core) old dbsteward conditionally ignores whitespace changes per sqlformat. is that necessary?
	return self.SqlFormat.Equals(other.SqlFormat) &&
		strings.EqualFold(self.Language, other.Language) &&
		self.Text == other.Text
}

func (self *FunctionDefinition) Validate(doc *Definition, schema *Schema, fn *Function) []error {
	out := []error{}
	if self.SqlFormat == "" {
		out = append(out, fmt.Errorf("function definition for %s.%s must have a sql format", schema.Name, fn.ShortSig()))
	}
	return out
}
