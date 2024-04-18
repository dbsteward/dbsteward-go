package model

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
)

type TriggerForEach string

const (
	TriggerForEachRow       TriggerForEach = "ROW"
	TriggerForEachStatement TriggerForEach = "STATEMENT"
)

func NewTriggerForEach(s string) (TriggerForEach, error) {
	if s == "" {
		return TriggerForEachStatement, nil
	}
	v := TriggerForEach(s)
	if v.Equals(TriggerForEachRow) {
		return TriggerForEachRow, nil
	}
	if v.Equals(TriggerForEachStatement) {
		return TriggerForEachStatement, nil
	}
	return "", fmt.Errorf("invalid trigger for each '%s'", s)
}

func (tfe TriggerForEach) Equals(other TriggerForEach) bool {
	return strings.EqualFold(string(tfe), string(other))
}

type TriggerTiming string

const (
	TriggerTimingBefore    TriggerTiming = "BEFORE"
	TriggerTimingAfter     TriggerTiming = "AFTER"
	TriggerTimingInsteadOf TriggerTiming = "INSTEAD OF"
)

func NewTriggerTiming(s string) (TriggerTiming, error) {
	v := TriggerTiming(s)
	if v.Equals(TriggerTimingBefore) {
		return TriggerTimingBefore, nil
	}
	if v.Equals(TriggerTimingAfter) {
		return TriggerTimingAfter, nil
	}
	if v.Equals(TriggerTimingInsteadOf) {
		return TriggerTimingInsteadOf, nil
	}
	return "", fmt.Errorf("invalid trigger timing '%s'", s)
}

func (tt TriggerTiming) Equals(other TriggerTiming) bool {
	return strings.EqualFold(string(tt), string(other))
}

// TODO(go,mysql) TODO(go,mssql) are there other constants here?

type Trigger struct {
	Name      string
	Table     string
	Events    []string
	Timing    TriggerTiming
	ForEach   TriggerForEach
	Function  string
	SqlFormat SqlFormat
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
		util.IStrsEq(self.Events, other.Events) &&
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

func (self *Trigger) Validate(*Definition, *Schema) []error {
	// TODO(go,3) validate values
	return nil
}
