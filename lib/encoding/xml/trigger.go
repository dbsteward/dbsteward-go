package xml

import "github.com/dbsteward/dbsteward/lib/model"

type Trigger struct {
	Name       string        `xml:"name,attr"`
	Table      string        `xml:"table,attr,omitempty"`
	Events     DelimitedList `xml:"event,attr"` // TODO(go,3) should be a dedicated type
	Timing     string        `xml:"when,attr"`  // XML when="", but not to be confused with the SQL WHEN clause, which isn't captured in this struct (yet) TODO(feat)
	ForEach    string        `xml:"forEach,attr"`
	Function   string        `xml:"function,attr"`
	SqlFormat  string        `xml:"sqlFormat,attr,omitempty"`
	SlonySetId *int          `xml:"slonySetId,attr,omitempty"`
}

func (self *Trigger) ToModel() (*model.Trigger, error) {
	panic("todo")
}
