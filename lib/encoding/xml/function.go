package xml

import (
	"fmt"
	"log/slog"

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

func FunctionsFromIR(l *slog.Logger, funcs []*ir.Function) ([]*Function, error) {
	if len(funcs) == 0 {
		return nil, nil
	}
	var rv []*Function
	for _, f := range funcs {
		if f != nil {
			ll := l.With(slog.String("function", f.Name))
			nf := Function{
				Name:            f.Name,
				Owner:           f.Owner,
				Description:     f.Description,
				Returns:         f.Returns,
				CachePolicy:     f.CachePolicy,
				ForceRedefine:   f.ForceRedefine,
				SecurityDefiner: f.SecurityDefiner,
				Parameters:      FunctionParametersFromIR(ll, f.Parameters),
				Definitions:     FunctionDefitionsFromIR(ll, f.Definitions),
			}
			var err error
			nf.Grants, err = GrantsFromIR(ll, f.Grants)
			if err != nil {
				return nil, err
			}
			rv = append(rv, &nf)
		}
	}
	return rv, nil
}

type FunctionParameter struct {
	Name      string `xml:"name,attr,omitempty"`
	Type      string `xml:"type,attr"`
	Direction string `xml:"direction,attr,omitempty"`
}

func FunctionParametersFromIR(l *slog.Logger, params []*ir.FunctionParameter) []*FunctionParameter {
	if len(params) == 0 {
		return nil
	}
	var rv []*FunctionParameter
	for _, param := range params {
		if param != nil {
			rv = append(
				rv,
				&FunctionParameter{
					Name:      param.Name,
					Type:      param.Type,
					Direction: string(param.Direction),
				},
			)
		}
	}
	return rv
}

func (fp *FunctionParameter) ToIR() (*ir.FunctionParameter, error) {
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

func FunctionDefitionsFromIR(l *slog.Logger, defs []*ir.FunctionDefinition) []*FunctionDefinition {
	if len(defs) == 0 {
		return nil
	}
	var rv []*FunctionDefinition
	for _, def := range defs {
		if def != nil {
			rv = append(
				rv,
				&FunctionDefinition{
					SqlFormat: string(def.SqlFormat),
					Language:  def.Language,
					Text:      def.Text,
				},
			)
		}
	}
	return rv
}

func (fd *FunctionDefinition) ToIR() (*ir.FunctionDefinition, error) {
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

func (f *Function) ToIR() (*ir.Function, error) {
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
		np, err := p.ToIR()
		if err != nil {
			return nil, fmt.Errorf("function '%s' invalid: %w", f.Name, err)
		}
		rv.Parameters = append(rv.Parameters, np)
	}
	for _, d := range f.Definitions {
		nd, err := d.ToIR()
		if err != nil {
			return nil, fmt.Errorf("function '%s' invalid: %w", f.Name, err)
		}
		rv.Definitions = append(rv.Definitions, nd)
	}
	for _, g := range f.Grants {
		ng, err := g.ToIR()
		if err != nil {
			return nil, fmt.Errorf("function '%s' invalid: %w", f.Name, err)
		}
		rv.Grants = append(rv.Grants, ng)
	}
	return &rv, nil
}
