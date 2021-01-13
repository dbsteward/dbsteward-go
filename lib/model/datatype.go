package model

type DataTypeKind string

const (
	DataTypeKindEnum      DataTypeKind = "enum"
	DataTypeKindComposite DataTypeKind = "composite"
	DataTypeKindDomain    DataTypeKind = "domain"
)

type DataType struct {
	Name              string                      `xml:"name,attr,omitempty"`
	Kind              DataTypeKind                `xml:"type,attr,omitempty"`
	SlonySetId        *int                        `xml:"slonySetId,attr,omitempty"`
	EnumValues        []string                    `xml:"enum"`
	CompositeFields   []*DataTypeCompositeField   `xml:"typeCompositeElement"`
	DomainType        *DataTypeDomainType         `xml:"domainType"`
	DomainConstraints []*DataTypeDomainConstraint `xml:"domainConstraint"`
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

func (self *DataType) Merge(overlay *DataType) {
	if overlay == nil {
		return
	}
	self.Kind = overlay.Kind
	self.SlonySetId = overlay.SlonySetId
	self.EnumValues = overlay.EnumValues
	self.CompositeFields = overlay.CompositeFields
	self.DomainType = overlay.DomainType
	self.DomainConstraints = overlay.DomainConstraints
}
