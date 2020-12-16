package model

import (
	"github.com/dbsteward/dbsteward/lib/format"
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
	SqlFormat format.SqlFormat `xml:"sqlFormat"`
	Language  string           `xml:"language"`
	Text      string           `xml:",chardata"`
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
