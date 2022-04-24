package xml

import "strings"

type Language struct {
	Name       string `xml:"name,attr"`
	Owner      string `xml:"owner,attr,omitempty"`
	Trusted    bool   `xml:"trusted,attr,omitempty"`
	Procedural bool   `xml:"procedural,attr,omitempty"`
	Handler    string `xml:"handler,attr,omitempty"`
	Validator  string `xml:"validator,attr,omitempty"`
}

func (self *Language) Merge(overlay *Language) {
	self.Owner = overlay.Owner
	self.Trusted = overlay.Trusted
	self.Procedural = overlay.Procedural
	self.Handler = overlay.Handler
	self.Validator = overlay.Validator
}

func (self *Language) IdentityMatches(other *Language) bool {
	if self == nil || other == nil {
		return false
	}
	return strings.EqualFold(self.Name, other.Name)
}

func (self *Language) Equals(other *Language) bool {
	if self == nil || other == nil {
		return false
	}
	return self.Trusted == other.Trusted &&
		self.Procedural == other.Procedural &&
		self.Handler == other.Handler &&
		self.Validator == other.Validator
}
