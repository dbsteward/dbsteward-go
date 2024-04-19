package xml

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/util"
)

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

func (s *Sequence) ToIR() (*ir.Sequence, error) {
	rv := ir.Sequence{
		Name:          s.Name,
		Owner:         s.Owner,
		Description:   s.Description,
		Cache:         util.SomePtr(s.Cache),
		Start:         util.SomePtr(s.Start),
		Min:           util.SomePtr(s.Min),
		Max:           util.SomePtr(s.Max),
		Increment:     util.SomePtr(s.Increment),
		Cycle:         s.Cycle,
		OwnedByColumn: s.OwnedByColumn,
	}

	for _, g := range s.Grants {
		ng, err := g.ToIR()
		if err != nil {
			return nil, fmt.Errorf("sequence '%s' invalid: %w", s.Name, err)
		}
		rv.Grants = append(rv.Grants, ng)
	}
	return &rv, nil
}
