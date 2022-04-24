package xml

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
)

type View struct {
	Name           string        `xml:"name,attr"`
	Description    string        `xml:"description,attr,omitempty"`
	Owner          string        `xml:"owner,attr,omitempty"`
	DependsOnViews DelimitedList `xml:"dependsOnViews,attr,omitempty"`
	SlonySetId     *int          `xml:"slonySetId,attr,omitempty"`
	Grants         []*Grant      `xml:"grant"`
	Queries        []*ViewQuery  `xml:"viewQuery"`
}

type ViewQuery struct {
	SqlFormat string `xml:"sqlFormat,attr,omitempty"`
	Text      string `xml:",cdata"`
}

func (self *View) GetOwner() string {
	return self.Owner
}

func (self *View) GetGrantsForRole(role string) []*Grant {
	out := []*Grant{}
	for _, grant := range self.Grants {
		if util.IStrsContains(grant.Roles, role) {
			out = append(out, grant)
		}
	}
	return out
}

func (self *View) GetGrants() []*Grant {
	return self.Grants
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
			if strings.EqualFold(baseQuery.SqlFormat, overlayQuery.SqlFormat) {
				baseQuery.Text = overlayQuery.Text
			}
		}
	}
}

func (self *View) IdentityMatches(other *View) bool {
	if self == nil || other == nil {
		return false
	}
	// TODO(go,3) take schema into account
	return strings.EqualFold(self.Name, other.Name)
}

func (self *View) Equals(other *View, sqlFormat string) bool {
	if self == nil || other == nil {
		return false
	}
	if strings.EqualFold(self.Owner, other.Owner) {
		return false
	}
	if !self.TryGetViewQuery(sqlFormat).Equals(other.TryGetViewQuery(sqlFormat)) {
		return false
	}
	return true
}

func (self *View) TryGetViewQuery(sqlFormat string) util.Opt[*ViewQuery] {
	return util.Find(self.Queries, func(q *ViewQuery) bool {
		return q.SqlFormat == sqlFormat
	})
}

func (self *ViewQuery) Equals(other *ViewQuery) bool {
	if self == nil || other == nil {
		return false
	}
	// TODO(feat) I'm not sure case-insensitive is correct here, but that's what I wrote 8 years ago in sql99_diff_views::is_view_modified
	return strings.EqualFold(self.GetNormalizedText(), other.GetNormalizedText())
}

func (self *ViewQuery) GetNormalizedText() string {
	// TODO(feat) can we clean this up a bit more? remove leading indents?
	return strings.TrimSuffix(strings.TrimSpace(self.Text), ";")
}