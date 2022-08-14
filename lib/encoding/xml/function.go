package xml

import "github.com/dbsteward/dbsteward/lib/model"

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

func (self *Function) ToModel() (*model.Function, error) {
	panic("todo")
}
