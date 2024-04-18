package xml

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/model"
)

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

func (t *Trigger) ToModel() (*model.Trigger, error) {
	if t == nil {
		return nil, nil
	}
	rv := model.Trigger{
		Name:     t.Name,
		Table:    t.Table,
		Events:   t.Events,
		Function: t.Function,
	}
	var err error
	rv.Timing, err = model.NewTriggerTiming(t.Timing)
	if err != nil {
		return nil, fmt.Errorf("invalid trigger '%s': %w", t.Name, err)
	}
	rv.ForEach, err = model.NewTriggerForEach(t.ForEach)
	if err != nil {
		return nil, fmt.Errorf("invalid trigger '%s': %w", t.Name, err)
	}
	rv.SqlFormat, err = model.NewSqlFormat(t.SqlFormat)
	if err != nil {
		return nil, fmt.Errorf("invalid trigger '%s': %w", t.Name, err)
	}
	return &rv, nil
}
