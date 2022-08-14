package xml

import "github.com/dbsteward/dbsteward/lib/model"

type Sequence struct {
	Name          string   `xml:"name,attr"`
	Owner         string   `xml:"owner,attr,omitempty"`
	Description   string   `xml:"description,attr,omitempty"`
	Cache         *int     `xml:"cache,attr,omitempty"`
	Start         *int     `xml:"start,attr,omitempty"`
	Min           *int     `xml:"min,attr,omitempty"`
	Max           *int     `xml:"max,attr,omitempty"`
	Increment     *int     `xml:"inc,attr,omitempty"`
	Cycle         bool     `xml:"cycle,attr,omitempty"`
	OwnedByColumn string   `xml:"ownedBy,attr,omitempty"`
	SlonyId       int      `xml:"slonyId,attr,omitempty"`
	SlonySetId    *int     `xml:"slonySetId,attr,omitempty"`
	Grants        []*Grant `xml:"grant"`
}

func (self *Sequence) ToModel() (*model.Sequence, error) {
	panic("todo")
}
