package xml

import (
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

func (self *Schema) ToModel() (*ir.Schema, error) {
	tables, err := util.MapErr(self.Tables, (*Table).ToModel)
	if err != nil {
		return nil, errors.Wrap(err, "could not process schema table tags")
	}
	grants, err := util.MapErr(self.Grants, (*Grant).ToModel)
	if err != nil {
		return nil, errors.Wrap(err, "could not process schema grant tags")
	}
	types, err := util.MapErr(self.Types, (*DataType).ToModel)
	if err != nil {
		return nil, errors.Wrap(err, "could not process schema type tags")
	}
	sequences, err := util.MapErr(self.Sequences, (*Sequence).ToModel)
	if err != nil {
		return nil, errors.Wrap(err, "could not process schema sequence tags")
	}
	functions, err := util.MapErr(self.Functions, (*Function).ToModel)
	if err != nil {
		return nil, errors.Wrap(err, "could not process schema function tags")
	}
	triggers, err := util.MapErr(self.Triggers, (*Trigger).ToModel)
	if err != nil {
		return nil, errors.Wrap(err, "could not process schema trigger tags")
	}
	views, err := util.MapErr(self.Views, (*View).ToModel)
	if err != nil {
		return nil, errors.Wrap(err, "could not process schema view tags")
	}

	return &ir.Schema{
		Name:        self.Name,
		Description: self.Description,
		Owner:       self.Owner,
		SlonySetId:  util.SomePtr(self.SlonySetId),
		Tables:      tables,
		Grants:      grants,
		Types:       types,
		Sequences:   sequences,
		Functions:   functions,
		Triggers:    triggers,
		Views:       views,
	}, nil
}
