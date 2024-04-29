package xml

import "github.com/dbsteward/dbsteward/lib/ir"

type DataType struct {
	Name              string                      `xml:"name,attr,omitempty"`
	Kind              string                      `xml:"type,attr,omitempty"`
	SlonySetId        *int                        `xml:"slonySetId,attr,omitempty"`
	EnumValues        []*DataTypeEnumValue        `xml:"enum"`
	CompositeFields   []*DataTypeCompositeField   `xml:"typeCompositeElement"`
	DomainType        *DataTypeDomainType         `xml:"domainType"`
	DomainConstraints []*DataTypeDomainConstraint `xml:"domainConstraint"`
}

type DataTypeEnumValue struct {
	Value string `xml:"name,attr"`
}

type DataTypeCompositeField struct {
	Name string `xml:"name,attr"`
	Type string `xml:"type,attr"`
}

type DataTypeDomainType struct {
	BaseType string `xml:"baseType,attr"`
	Default  string `xml:"default,attr,omitempty"`
	Nullable bool   `xml:"null,attr,omitempty"`
}

type DataTypeDomainConstraint struct {
	Name  string `xml:"name,attr,omitempty"`
	Check string `xml:",chardata"`
}

func (self *DataType) ToIR() (*ir.TypeDef, error) {
	panic("todo")
}
