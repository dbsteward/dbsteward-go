package model

import "strings"

type Trigger struct {
	Name      string        `xml:"name,attr"`
	Table     string        `xml:"table,attr"`
	Event     DelimitedList `xml:"event,attr"`
	When      string        `xml:"when,attr"`
	ForEach   string        `xml:"forEach,attr"`
	Function  string        `xml:"function,attr"`
	SqlFormat SqlFormat     `xml:"sqlFormat,attr"`
}

func (self *Trigger) AddEvent(event string) {
	// TODO(feat) sanity check
	self.Event = append(self.Event, event)
}

func (self *Trigger) IdentityMatches(other *Trigger) bool {
	// TODO(feat) this doesn't take schema into account
	// two identically named triggers in different schemas for identically named tables in their respective schemas shouldn't be equal... should they?
	return strings.EqualFold(self.Name, other.Name) && strings.EqualFold(self.Table, other.Table)
}

func (self *Trigger) Merge(overlay *Trigger) {
	self.Event = overlay.Event
	self.When = overlay.When
	self.ForEach = overlay.ForEach
	self.Function = overlay.Function
	self.SqlFormat = overlay.SqlFormat
}
