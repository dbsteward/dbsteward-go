package xml

import (
	"log/slog"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/pkg/errors"
)

type Schema struct {
	Name        string      `xml:"name,attr"`
	Description string      `xml:"description,attr,omitempty"`
	Owner       string      `xml:"owner,attr,omitempty"`
	SlonySetId  *int        `xml:"slonySetId,attr,omitempty"`
	Tables      []*Table    `xml:"table"`
	Grants      []*Grant    `xml:"grant"`
	Types       []*DataType `xml:"type"`
	Sequences   []*Sequence `xml:"sequence"`
	Functions   []*Function `xml:"function"`
	Triggers    []*Trigger  `xml:"trigger"`
	Views       []*View     `xml:"view"`
}

func SchemasFromIR(l *slog.Logger, in []*ir.Schema) ([]*Schema, error) {
	l.Debug("start converting schemas")
	defer l.Debug("done converting schemas")
	if len(in) == 0 {
		return nil, nil
	}
	var rv []*Schema
	for _, irsch := range in {
		if irsch != nil {
			nsch, err := SchemaFromIR(l, irsch)
			if err != nil {
				return nil, err
			}
			rv = append(rv, nsch)
		}
	}
	return rv, nil
}

func SchemaFromIR(l *slog.Logger, in *ir.Schema) (*Schema, error) {
	l = l.With(slog.String("schema", in.Name))
	l.Debug("start converting schema")
	defer l.Debug("done converting schema")
	rv := Schema{
		Name:        in.Name,
		Description: in.Description,
		Owner:       in.Owner,
		SlonySetId:  in.SlonySetId.Ptr(),
	}
	var err error
	rv.Tables, err = TablesFromIR(l, in.Tables)
	if err != nil {
		return nil, err
	}
	rv.Grants, err = GrantsFromIR(l, in.Grants)
	if err != nil {
		return nil, err
	}
	rv.Types, err = TypesFromIR(l, in.Types)
	if err != nil {
		return nil, err
	}
	rv.Sequences, err = SequencesFromIR(l, in.Sequences)
	if err != nil {
		return nil, err
	}
	rv.Functions, err = FunctionsFromIR(l, in.Functions)
	if err != nil {
		return nil, err
	}
	rv.Views, err = ViewsFromIR(l, in.Views)
	if err != nil {
		return nil, err
	}
	return &rv, nil
}

func (sch *Schema) ToIR() (*ir.Schema, error) {
	tables, err := util.MapErr(sch.Tables, (*Table).ToIR)
	if err != nil {
		return nil, errors.Wrap(err, "could not process schema table tags")
	}
	grants, err := util.MapErr(sch.Grants, (*Grant).ToIR)
	if err != nil {
		return nil, errors.Wrap(err, "could not process schema grant tags")
	}
	types, err := util.MapErr(sch.Types, (*DataType).ToIR)
	if err != nil {
		return nil, errors.Wrap(err, "could not process schema type tags")
	}
	sequences, err := util.MapErr(sch.Sequences, (*Sequence).ToIR)
	if err != nil {
		return nil, errors.Wrap(err, "could not process schema sequence tags")
	}
	functions, err := util.MapErr(sch.Functions, (*Function).ToIR)
	if err != nil {
		return nil, errors.Wrap(err, "could not process schema function tags")
	}
	triggers, err := util.MapErr(sch.Triggers, (*Trigger).ToIR)
	if err != nil {
		return nil, errors.Wrap(err, "could not process schema trigger tags")
	}
	views, err := util.MapErr(sch.Views, (*View).ToIR)
	if err != nil {
		return nil, errors.Wrap(err, "could not process schema view tags")
	}

	return &ir.Schema{
		Name:        sch.Name,
		Description: sch.Description,
		Owner:       sch.Owner,
		SlonySetId:  util.SomePtr(sch.SlonySetId),
		Tables:      tables,
		Grants:      grants,
		Types:       types,
		Sequences:   sequences,
		Functions:   functions,
		Triggers:    triggers,
		Views:       views,
	}, nil
}
