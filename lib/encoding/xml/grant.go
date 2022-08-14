package xml

import "github.com/dbsteward/dbsteward/lib/model"

type Grant struct {
	Roles       DelimitedList      `xml:"role,attr,omitempty"`
	Permissions CommaDelimitedList `xml:"operation,attr,omitempty"`
	With        string             `xml:"with,attr,omitempty"`
}

func (self *Grant) ToModel() (*model.Grant, error) {
	panic("todo")
}
