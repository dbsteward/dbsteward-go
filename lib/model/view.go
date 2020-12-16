package model

type View struct {
	Name        string       `xml:"name,attr"`
	Description string       `xml:"description,attr"`
	Owner       string       `xml:"owner,attr"`
	Grants      []*Grant     `xml:"grant"`
	Queries     []*ViewQuery `xml:"viewQuery"`
}

type ViewQuery struct {
	SqlFormat SqlFormat `xml:"sqlFormat,attr"`
	Text      string    `xml:",chardata"`
}

func (self *View) GetGrantsForRole(role string) []*Grant {
	out := []*Grant{}
	for _, grant := range self.Grants {
		if grant.Role == role {
			out = append(out, grant)
		}
	}
	return out
}

func (self *View) AddGrant(grant *Grant) {
	// TODO(feat) sanity check
	self.Grants = append(self.Grants, grant)
}
