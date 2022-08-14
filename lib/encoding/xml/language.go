package xml

import "github.com/dbsteward/dbsteward/lib/model"

type Language struct {
	Name       string `xml:"name,attr"`
	Owner      string `xml:"owner,attr,omitempty"`
	Trusted    bool   `xml:"trusted,attr,omitempty"`
	Procedural bool   `xml:"procedural,attr,omitempty"`
	Handler    string `xml:"handler,attr,omitempty"`
	Validator  string `xml:"validator,attr,omitempty"`
}

func (self *Language) ToModel() (*model.Language, error) {
	panic("todo")
}
