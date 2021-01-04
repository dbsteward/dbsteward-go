package model

type DataTypeKind string

const (
	DataTypeKindEnum      DataTypeKind = "enum"
	DataTypeKindComposite DataTypeKind = "composite"
	DataTypeKindDomain    DataTypeKind = "domain"
)

type DataType struct {
	Name              string                    `xml:"name,attr"`
	Type              DataTypeKind              `xml:"type,attr"`
	SlonySetId        int                       `xml:"slonySetId,attr"`
	EnumValues        []string                  `xml:"enum"`
	CompositeFields   []*DataTypeCompositeField `xml:"typeCompositeElement"`
	DomainType        *DataTypeDomainType       `xml:"domainType"`
	DomainConstraints []string                  `xml:"domainConstraint"`
}

type DataTypeCompositeField struct {
	Name string `xml:"name,attr"`
	Type string `xml:"type,attr"`
}

type DataTypeDomainType struct {
	BaseType string `xml:"baseType,attr"`
	Default  string `xml:"default,attr"`
	Null     bool   `xml:"null,attr"`
}

func (self *DataType) Merge(overlay *DataType) {
	if overlay == nil {
		return
	}
	self.Type = overlay.Type
	self.SlonySetId = overlay.SlonySetId
	self.EnumValues = overlay.EnumValues
	self.CompositeFields = overlay.CompositeFields
	self.DomainType = overlay.DomainType
	self.DomainConstraints = overlay.DomainConstraints
}
