package xml

import (
	"fmt"
	"log/slog"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/util"
)

type Sequence struct {
	Name          string `xml:"name,attr"`
	Owner         string `xml:"owner,attr,omitempty"`
	Description   string `xml:"description,attr,omitempty"`
	Cache         *int   `xml:"cache,attr,omitempty"`
	Start         *int   `xml:"start,attr,omitempty"`
	Min           *int   `xml:"min,attr,omitempty"`
	Max           *int   `xml:"max,attr,omitempty"`
	Increment     *int   `xml:"inc,attr,omitempty"`
	Cycle         bool   `xml:"cycle,attr,omitempty"`
	OwnedBySchema string
	OwnedByTable  string
	OwnedByColumn string
	SlonyId       int      `xml:"slonyId,attr,omitempty"`
	SlonySetId    *int     `xml:"slonySetId,attr,omitempty"`
	Grants        []*Grant `xml:"grant"`
}

func SequencesFromIR(l *slog.Logger, seqs []*ir.Sequence) ([]*Sequence, error) {
	if len(seqs) == 0 {
		return nil, nil
	}
	var rv []*Sequence
	for _, seq := range seqs {
		if seq != nil {
			ll := l.With(slog.String("sequence", seq.Name))
			ns := Sequence{
				Name:          seq.Name,
				Owner:         seq.Owner,
				Description:   seq.Description,
				Cache:         seq.Cache.Ptr(),
				Start:         seq.Start.Ptr(),
				Min:           seq.Min.Ptr(),
				Max:           seq.Max.Ptr(),
				Increment:     seq.Increment.Ptr(),
				Cycle:         seq.Cycle,
				OwnedBySchema: seq.OwnedBySchema,
				OwnedByTable:  seq.OwnedByTable,
				OwnedByColumn: seq.OwnedByColumn,
			}
			var err error
			ns.Grants, err = GrantsFromIR(ll, seq.Grants)
			if err != nil {
				return nil, err
			}
			rv = append(rv, &ns)
		}
	}
	return rv, nil
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
		OwnedBySchema: s.OwnedBySchema,
		OwnedByTable:  s.OwnedByTable,
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
