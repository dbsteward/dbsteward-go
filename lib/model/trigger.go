package model

import (
)

type Trigger struct {
	Name      string           `xml:"name,attr"`
	Table     string           `xml:"table,attr"`
	Event     DelimitedList    `xml:"event,attr"`
	When      string           `xml:"when,attr"`
	ForEach   string           `xml:"forEach,attr"`
	Function  string           `xml:"function,attr"`
	SqlFormat SqlFormat `xml:"sqlFormat,attr"`
}

func (self *Trigger) AddEvent(event string) {
	// TODO(feat) sanity check
	self.Event = append(self.Event, event)
}
