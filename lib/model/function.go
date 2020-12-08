package model

import (
	"github.com/dbsteward/dbsteward/lib/format"
)

// TODO(go, core) finish fleshing this out

type Function struct {
	Name       string                `xml:"name"`
	Definition []*FunctionDefinition `xml:"functionDefinition"`
	Grants     []*Grant              `xml:"grant"`
	Revokes    []*Revoke             `xml:"revoke"`
}

type FunctionDefinition struct {
	SqlFormat format.SqlFormat `xml:"sqlFormat"`
	Language  string           `xml:"language"`
}

func (self *Function) HasDefinition() bool {
	// TODO(go,core)
	return false
}

func (self *Function) TryGetDefinition() (*FunctionDefinition, bool) {
	// TODO(go,core) see pgsql8_function::has_definition and get_definition
	return nil, false
}
