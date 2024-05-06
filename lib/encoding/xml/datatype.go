package xml

import (
	"log/slog"

	"github.com/dbsteward/dbsteward/lib/ir"
)

type DataType struct {
	Name              string                      `xml:"name,attr,omitempty"`
	Kind              string                      `xml:"type,attr,omitempty"`
	SlonySetId        *int                        `xml:"slonySetId,attr,omitempty"`
	EnumValues        []*DataTypeEnumValue        `xml:"enum"`
	CompositeFields   []*DataTypeCompositeField   `xml:"typeCompositeElement"`
	DomainType        *DataTypeDomainType         `xml:"domainType"`
	DomainConstraints []*DataTypeDomainConstraint `xml:"domainConstraint"`
}

func TypesFromIR(l *slog.Logger, types []*ir.TypeDef) ([]*DataType, error) {
	if len(types) == 0 {
		return nil, nil
	}
	var rv []*DataType
	for _, t := range types {
		if t != nil {
			ndt, err := TypeFromIR(l, t)
			if err != nil {
				return nil, err
			}
			rv = append(rv, ndt)
		}
	}
	return rv, nil
}

func TypeFromIR(l *slog.Logger, t *ir.TypeDef) (*DataType, error) {
	l = l.With(slog.String("TypeDef", t.Name))
	ndt := DataType{
		Name: t.Name,
		Kind: t.Kind.String(),
		// SlonySetId Not in the IR
		EnumValues:        DataTypeEnumValuesFromIR(l, t.EnumValues),
		CompositeFields:   DataTypeCompositFieldsFromIR(l, t.CompositeFields),
		DomainType:        DataTypeDomainTypeFromIR(l, t.DomainType),
		DomainConstraints: DataTypeDomainConstraintsFromIR(l, t.DomainConstraints),
	}
	return &ndt, nil
}

type DataTypeEnumValue struct {
	Value string `xml:"name,attr"`
}

func DataTypeEnumValuesFromIR(l *slog.Logger, vals []ir.DataTypeEnumValue) []*DataTypeEnumValue {
	if len(vals) == 0 {
		return nil
	}
	var rv []*DataTypeEnumValue
	for _, val := range vals {
		rv = append(
			rv,
			&DataTypeEnumValue{Value: string(val)},
		)
	}
	return rv
}

type DataTypeCompositeField struct {
	Name string `xml:"name,attr"`
	Type string `xml:"type,attr"`
}

func DataTypeCompositFieldsFromIR(l *slog.Logger, vals []ir.DataTypeCompositeField) []*DataTypeCompositeField {
	if len(vals) == 0 {
		return nil
	}
	var rv []*DataTypeCompositeField
	for _, val := range vals {
		rv = append(
			rv,
			&DataTypeCompositeField{
				Name: val.Name,
				Type: val.Type,
			},
		)
	}
	return rv
}

type DataTypeDomainType struct {
	BaseType string `xml:"baseType,attr"`
	Default  string `xml:"default,attr,omitempty"`
	Nullable bool   `xml:"null,attr,omitempty"`
}

func DataTypeDomainTypeFromIR(l *slog.Logger, d *ir.DataTypeDomainType) *DataTypeDomainType {
	if d == nil {
		return nil
	}
	return &DataTypeDomainType{
		BaseType: d.BaseType,
		Default:  d.Default,
		Nullable: d.Nullable,
	}
}

type DataTypeDomainConstraint struct {
	Name  string `xml:"name,attr,omitempty"`
	Check string `xml:",chardata"`
}

func DataTypeDomainConstraintsFromIR(l *slog.Logger, cons []ir.DataTypeDomainConstraint) []*DataTypeDomainConstraint {
	if len(cons) == 0 {
		return nil
	}
	var rv []*DataTypeDomainConstraint
	for _, con := range cons {
		rv = append(
			rv,
			&DataTypeDomainConstraint{
				Name:  con.Name,
				Check: con.Check,
			},
		)
	}
	return rv
}

func (dt *DataType) ToIR() (*ir.TypeDef, error) {
	panic("todo")
}
