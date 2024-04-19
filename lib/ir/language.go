package ir

type Language struct {
	Name       string
	Owner      string
	Trusted    bool
	Procedural bool
	Handler    string
	Validator  string
}

func (self *Language) Merge(overlay *Language) {
	self.Owner = overlay.Owner
	self.Trusted = overlay.Trusted
	self.Procedural = overlay.Procedural
	self.Handler = overlay.Handler
	self.Validator = overlay.Validator
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
