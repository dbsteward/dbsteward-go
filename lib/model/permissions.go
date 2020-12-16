package model

type Grant struct {
	Role      string        `xml:"role,attr"`
	Operation DelimitedList `xml:"operation,attr"`
	With      DelimitedList `xml:"with,attr"`
}

type Revoke struct {
}

func (self *Grant) AddOperation(op string) {
	self.Operation = append(self.Operation, op)
}

func (self *Grant) SetCanGrant(canGrant bool) {
	// TODO(feat) sanity check
	// TODO(go,core) remove if false
	if canGrant {
		self.With = append(self.With, "GRANT")
	}
}
