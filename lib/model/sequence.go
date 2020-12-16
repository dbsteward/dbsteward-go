package model

type Sequence struct {
	Name  string `xml:"name,attr"`
	Owner string `xml:"owner,attr"`
	// TODO(go,nth) these should probably be ints for completenesss sake
	Cache     string   `xml:"cache,attr"`
	Start     string   `xml:"start,attr"`
	Min       string   `xml:"min,attr"`
	Max       string   `xml:"max,attr"`
	Increment string   `xml:"inc,attr"`
	Cycle     bool     `xml:"cycle,attr"`
	Grants    []*Grant `xml:"grant"`
}

func (self *Sequence) GetGrantsForRole(role string) []*Grant {
	out := []*Grant{}
	for _, grant := range self.Grants {
		if grant.Role == role {
			out = append(out, grant)
		}
	}
	return out
}

func (self *Sequence) AddGrant(grant *Grant) {
	// TODO(feat) sanity check
	self.Grants = append(self.Grants, grant)
}
