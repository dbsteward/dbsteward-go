package model

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
)

type Sequence struct {
	Name          string
	Owner         string
	Description   string
	Cache         util.Opt[int]
	Start         util.Opt[int]
	Min           util.Opt[int]
	Max           util.Opt[int]
	Increment     util.Opt[int]
	Cycle         bool
	OwnedByColumn string
	Grants        []*Grant
}

func (self *Sequence) GetGrantsForRole(role string) []*Grant {
	out := []*Grant{}
	for _, grant := range self.Grants {
		if util.IStrsContains(grant.Roles, role) {
			out = append(out, grant)
		}
	}
	return out
}

func (self *Sequence) GetGrants() []*Grant {
	return self.Grants
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

func (self *Sequence) IdentityMatches(other *Sequence) bool {
	if self == nil || other == nil {
		return false
	}
	return strings.EqualFold(self.Name, other.Name)
}

func (self *Sequence) Validate(*Definition, *Schema) []error {
	// TODO(go,3) validate owner, remove from other codepaths
	// TODO(go,3) validate cache/start/min/max/increment values
	// TODO(go,3) validate grants, remove from other codepaths
	return nil
}
