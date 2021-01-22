package model

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
)

type TriggerForEach string

const (
	TriggerForEachRow       TriggerForEach = "ROW"
	TriggerForEachStatement TriggerForEach = "STATEMENT"
)

func (self TriggerForEach) Equals(other TriggerForEach) bool {
	return strings.EqualFold(string(self), string(other))
}

type TriggerTiming string

const (
	TriggerTimingBefore    TriggerTiming = "BEFORE"
	TriggerTimingAfter     TriggerTiming = "AFTER"
	TriggerTimingInsteadOf TriggerTiming = "INSTEAD OF"
)

func (self TriggerTiming) Equals(other TriggerTiming) bool {
	return strings.EqualFold(string(self), string(other))
}

// TODO(go,mysql) TODO(go,mssql) are there other constants here?

type Trigger struct {
	Name       string         `xml:"name,attr"`
	Table      string         `xml:"table,attr,omitempty"`
	Events     DelimitedList  `xml:"event,attr"` // TODO(go,3) should be a dedicated type
	Timing     TriggerTiming  `xml:"when,attr"`  // Not to be confused with the WHEN clause
	ForEach    TriggerForEach `xml:"forEach,attr"`
	Function   string         `xml:"function,attr"`
	SqlFormat  SqlFormat      `xml:"sqlFormat,attr,omitempty"`
	SlonySetId *int           `xml:"slonySetId,attr,omitempty"`
}

func (self *Trigger) AddEvent(event string) {
	// TODO(feat) sanity check
	self.Events = append(self.Events, event)
}

func (self *Trigger) IdentityMatches(other *Trigger) bool {
	if self == nil || other == nil {
		return false
	}
	// TODO(feat) this doesn't take schema into account
	// two identically named triggers in different schemas for identically named tables in their respective schemas shouldn't be equal... should they?
	return strings.EqualFold(self.Name, other.Name) && strings.EqualFold(self.Table, other.Table)
}

func (self *Trigger) Equals(other *Trigger) bool {
	if self == nil || other == nil {
		return false
	}
	// TODO(feat) should this include identity?
	return self.IdentityMatches(other) &&
		strings.EqualFold(self.Function, other.Function) &&
		util.StrsIEq(self.Events, other.Events) &&
		self.ForEach.Equals(other.ForEach) &&
		self.Timing.Equals(other.Timing) &&
		self.SqlFormat.Equals(other.SqlFormat)
}

func (self *Trigger) Merge(overlay *Trigger) {
	self.Events = overlay.Events
	self.Timing = overlay.Timing
	self.ForEach = overlay.ForEach
	self.Function = overlay.Function
	self.SqlFormat = overlay.SqlFormat
}
