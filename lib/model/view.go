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

func (self *View) Merge(overlay *View) {
	self.Description = overlay.Description
	self.Owner = overlay.Owner

	for _, grant := range overlay.Grants {
		self.AddGrant(grant)
	}

	for _, overlayQuery := range overlay.Queries {
		for _, baseQuery := range self.Queries {
			if baseQuery.SqlFormat.Equals(overlayQuery.SqlFormat) {
				baseQuery.Text = overlayQuery.Text
			}
		}
	}
}
