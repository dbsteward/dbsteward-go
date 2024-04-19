package xml

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/ir"
)

type Function struct {
	Name            string                `xml:"name,attr"`
	Owner           string                `xml:"owner,attr,omitempty"`
	Description     string                `xml:"description,attr,omitempty"`
	Returns         string                `xml:"returns,attr"`
	CachePolicy     string                `xml:"cachePolicy,attr,omitempty"`
	ForceRedefine   bool                  `xml:"forceRedefine,attr,omitempty"`
	SecurityDefiner bool                  `xml:"securityDefiner,attr,omitempty"`
	SlonySetId      *int                  `xml:"slonySetId,attr,omitempty"`
	Parameters      []*FunctionParameter  `xml:"functionParameter"`
	Definitions     []*FunctionDefinition `xml:"functionDefinition"`
	Grants          []*Grant              `xml:"grant"`
}

type FunctionParameter struct {
	Name      string `xml:"name,attr,omitempty"`
	Type      string `xml:"type,attr"`
	Direction string `xml:"direction,attr,omitempty"`
}

func (fp *FunctionParameter) ToModel() (*ir.FunctionParameter, error) {
	if fp == nil {
		return nil, nil
	}
	rv := ir.FunctionParameter{
		Name: fp.Name,
		Type: fp.Type,
	}
	var err error
	rv.Direction, err = ir.NewFuncParamDir(fp.Direction)
	if err != nil {
		return nil, fmt.Errorf("function parameter '%s' invalid: %w", fp.Name, err)
	}
	return &rv, nil
}

type FunctionDefinition struct {
	SqlFormat string `xml:"sqlFormat,attr,omitempty"`
	Language  string `xml:"language,attr,omitempty"`
	Text      string `xml:",cdata"`
}

func (fd *FunctionDefinition) ToModel() (*ir.FunctionDefinition, error) {
	if fd == nil {
		return nil, nil
	}
	rv := ir.FunctionDefinition{
		Language: fd.Language,
		Text:     fd.Text,
	}
	var err error
	rv.SqlFormat, err = ir.NewSqlFormat(fd.SqlFormat)
	if err != nil {
		return nil, err
	}
	return &rv, nil
}

func (f *Function) ToModel() (*ir.Function, error) {
	if f == nil {
		return nil, nil
	}
	rv := ir.Function{
		Name:            f.Name,
		Owner:           f.Owner,
		Description:     f.Description,
		Returns:         f.Returns,
		CachePolicy:     f.CachePolicy,
		ForceRedefine:   f.ForceRedefine,
		SecurityDefiner: f.SecurityDefiner,
	}
	for _, p := range f.Parameters {
		np, err := p.ToModel()
		if err != nil {
			return nil, fmt.Errorf("function '%s' invalid: %w", f.Name, err)
		}
		rv.Parameters = append(rv.Parameters, np)
	}
	for _, d := range f.Definitions {
		nd, err := d.ToModel()
		if err != nil {
			return nil, fmt.Errorf("function '%s' invalid: %w", f.Name, err)
		}
		rv.Definitions = append(rv.Definitions, nd)
	}
	for _, g := range f.Grants {
		ng, err := g.ToModel()
		if err != nil {
			return nil, fmt.Errorf("function '%s' invalid: %w", f.Name, err)
		}
		rv.Grants = append(rv.Grants, ng)
	}
	return &rv, nil
}
