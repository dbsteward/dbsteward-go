package xml

import (
	"fmt"
	"log/slog"

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

func ViewsFromIR(l *slog.Logger, views []*ir.View) ([]*View, error) {
	if len(views) == 0 {
		return nil, nil
	}
	var rv []*View
	for _, view := range views {
		if view != nil {
			ll := l.With(slog.String("view", view.Name))
			nv := View{
				Name:           view.Name,
				Description:    view.Description,
				Owner:          view.Owner,
				DependsOnViews: view.DependsOnViews,
				Queries:        ViewQueriesFromIR(ll, view.Queries),
			}
			var err error
			nv.Grants, err = GrantsFromIR(ll, view.Grants)
			if err != nil {
				return nil, err
			}
			rv = append(rv, &nv)
		}
	}
	return rv, nil
}

type ViewQuery struct {
	SqlFormat string `xml:"sqlFormat,attr,omitempty"`
	Text      string `xml:",cdata"`
}

func ViewQueriesFromIR(l *slog.Logger, queries []*ir.ViewQuery) []*ViewQuery {
	if len(queries) == 0 {
		return nil
	}
	var rv []*ViewQuery
	for _, query := range queries {
		if query != nil {
			rv = append(
				rv,
				&ViewQuery{
					SqlFormat: string(query.SqlFormat),
					Text:      query.Text,
				},
			)
		}
	}
	return rv
}

func (vq *ViewQuery) ToIR() (*ir.ViewQuery, error) {
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

func (v *View) ToIR() (*ir.View, error) {
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
		ng, err := g.ToIR()
		if err != nil {
			return nil, fmt.Errorf("invalid view '%s': %w", v.Name, err)
		}
		rv.Grants = append(rv.Grants, ng)
	}
	for _, q := range v.Queries {
		nq, err := q.ToIR()
		if err != nil {
			return nil, fmt.Errorf("invalid view '%s': %w", v.Name, err)
		}
		rv.Queries = append(rv.Queries, nq)
	}
	return &rv, nil
}
