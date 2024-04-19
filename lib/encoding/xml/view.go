package xml

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/ir"
)

type View struct {
	Name           string        `xml:"name,attr"`
	Description    string        `xml:"description,attr,omitempty"`
	Owner          string        `xml:"owner,attr,omitempty"`
	DependsOnViews DelimitedList `xml:"dependsOnViews,attr,omitempty"`
	SlonySetId     *int          `xml:"slonySetId,attr,omitempty"`
	Grants         []*Grant      `xml:"grant"`
	Queries        []*ViewQuery  `xml:"viewQuery"`
}

type ViewQuery struct {
	SqlFormat string `xml:"sqlFormat,attr,omitempty"`
	Text      string `xml:",cdata"`
}

func (vq *ViewQuery) ToModel() (*ir.ViewQuery, error) {
	if vq == nil {
		return nil, nil
	}
	rv := ir.ViewQuery{
		Text: vq.Text,
	}
	var err error
	rv.SqlFormat, err = ir.NewSqlFormat(vq.SqlFormat)
	if err != nil {
		return nil, err
	}
	return &rv, nil
}

func (v *View) ToModel() (*ir.View, error) {
	if v == nil {
		return nil, nil
	}
	rv := ir.View{
		Name:           v.Name,
		Description:    v.Description,
		Owner:          v.Owner,
		DependsOnViews: v.DependsOnViews,
	}
	for _, g := range v.Grants {
		ng, err := g.ToModel()
		if err != nil {
			return nil, fmt.Errorf("invalid view '%s': %w", v.Name, err)
		}
		rv.Grants = append(rv.Grants, ng)
	}
	for _, q := range v.Queries {
		nq, err := q.ToModel()
		if err != nil {
			return nil, fmt.Errorf("invalid view '%s': %w", v.Name, err)
		}
		rv.Queries = append(rv.Queries, nq)
	}
	return &rv, nil
}
