package model

type Language struct {
	Name       string `xml:"name,attr"`
	Owner      string `xml:"owner,attr"`
	Trusted    bool   `xml:"trusted,attr"`
	Procedural bool   `xml:"procedural,attr"`
	Handler    string `xml:"handler,attr"`
	Validator  string `xml:"validator,attr"`
}

func (self *Language) Merge(overlay *Language) {
	self.Owner = overlay.Owner
	self.Trusted = overlay.Trusted
	self.Procedural = overlay.Procedural
	self.Handler = overlay.Handler
	self.Validator = overlay.Validator
}
