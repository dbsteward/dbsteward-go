package model

import "strings"

type TriggerForEach string

const (
	TriggerForEachRow       TriggerForEach = "ROW"
	TriggerForEachStatement TriggerForEach = "STATEMENT"
)

type TriggerTiming string

const (
	TriggerTimingBefore    TriggerTiming = "BEFORE"
	TriggerTimingAfter     TriggerTiming = "AFTER"
	TriggerTimingInsteadOf TriggerTiming = "INSTEAD OF"
)

// TODO(go,mysql) TODO(go,mssql) are there other constants here?

type Trigger struct {
	Name      string         `xml:"name,attr"`
	Table     string         `xml:"table,attr"`
	Events    DelimitedList  `xml:"event,attr"` // TODO(go,3) should be a dedicated type
	Timing    TriggerTiming  `xml:"when,attr"`  // Not to be confused with the WHEN clause
	ForEach   TriggerForEach `xml:"forEach,attr"`
	Function  string         `xml:"function,attr"`
	SqlFormat SqlFormat      `xml:"sqlFormat,attr"`
}

func (self *Trigger) AddEvent(event string) {
	// TODO(feat) sanity check
	self.Events = append(self.Events, event)
}

func (self *Trigger) IdentityMatches(other *Trigger) bool {
	// TODO(feat) this doesn't take schema into account
	// two identically named triggers in different schemas for identically named tables in their respective schemas shouldn't be equal... should they?
	return strings.EqualFold(self.Name, other.Name) && strings.EqualFold(self.Table, other.Table)
}

func (self *Trigger) Merge(overlay *Trigger) {
	self.Events = overlay.Events
	self.Timing = overlay.Timing
	self.ForEach = overlay.ForEach
	self.Function = overlay.Function
	self.SqlFormat = overlay.SqlFormat
}
