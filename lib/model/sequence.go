package model

import (
	"github.com/dbsteward/dbsteward/lib/util"
)

type Sequence struct {
	Name          string   `xml:"name,attr"`
	Owner         string   `xml:"owner,attr"`
	Description   string   `xml:"description,attr"`
	Cache         *int     `xml:"cache,attr"`
	Start         *int     `xml:"start,attr"`
	Min           *int     `xml:"min,attr"`
	Max           *int     `xml:"max,attr"`
	Increment     *int     `xml:"inc,attr"`
	Cycle         bool     `xml:"cycle,attr"`
	OwnedByColumn string   `xml:"ownedBy,attr"`
	SlonyId       int      `xml:"slonyId,attr"`
	SlonySetId    int      `xml:"slonySetId,attr"`
	Grants        []*Grant `xml:"grant"`
}

func (self *Sequence) GetGrantsForRole(role string) []*Grant {
	out := []*Grant{}
	for _, grant := range self.Grants {
		if util.IIndexOfStr(role, grant.Roles) >= 0 {
			out = append(out, grant)
		}
	}
	return out
}

func (self *Sequence) AddGrant(grant *Grant) {
	// TODO(feat) sanity check
	self.Grants = append(self.Grants, grant)
}

func (self *Sequence) Merge(overlay *Sequence) {
	if overlay == nil {
		return
	}

	self.Owner = overlay.Owner
	self.Cache = overlay.Cache
	self.Start = overlay.Start
	self.Min = overlay.Min
	self.Max = overlay.Max
	self.Increment = overlay.Increment
	self.Cycle = overlay.Cycle

	for _, overlayGrant := range overlay.Grants {
		self.AddGrant(overlayGrant)
	}
}
