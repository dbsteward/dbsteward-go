package ir

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
)

type View struct {
	Name           string
	Description    string
	Owner          string
	DependsOnViews []string
	Grants         []*Grant
	Queries        []*ViewQuery
}

type ViewQuery struct {
	SqlFormat SqlFormat
	Text      string
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
			if baseQuery.SqlFormat.Equals(overlayQuery.SqlFormat) {
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

func (self *View) Equals(other *View, sqlFormat SqlFormat) bool {
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

func (self *View) Validate(*Definition, *Schema) []error {
	// TODO(go,3) validate owner, remove from other codepaths
	// TODO(go,3) validate ViewQueries
	return nil
}

func (self *View) TryGetViewQuery(sqlFormat SqlFormat) *ViewQuery {
	for _, query := range self.Queries {
		if query.SqlFormat.Equals(sqlFormat) {
			return query
		}
	}
	return nil
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

type ViewRef struct {
	Schema *Schema
	View   *View
}

func (self ViewRef) String() string {
	return fmt.Sprintf("%s.%s", self.Schema.Name, self.View.Name)
}
