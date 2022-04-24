package xml

type Grant struct {
	Roles       DelimitedList      `xml:"role,attr,omitempty"`
	Permissions CommaDelimitedList `xml:"operation,attr,omitempty"`
	With        string             `xml:"with,attr,omitempty"`
}

func (self *Grant) AddPermission(op string) {
	self.Permissions = append(self.Permissions, op)
}
