package xml

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
)

type DataTypeKind string

const (
	DataTypeKindEnum      DataTypeKind = "enum"
	DataTypeKindComposite DataTypeKind = "composite"
	DataTypeKindDomain    DataTypeKind = "domain"
)

func (self DataTypeKind) Equals(other DataTypeKind) bool {
	return strings.EqualFold(string(self), string(other))
}

type DataType struct {
	Name              string                      `xml:"name,attr,omitempty"`
	Kind              DataTypeKind                `xml:"type,attr,omitempty"`
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

func (self *DataType) TryGetDomainConstraintNamed(name string) *DataTypeDomainConstraint {
	util.Assert(self.Kind.Equals(DataTypeKindDomain), "can only be called for Domain kind")
	for _, constraint := range self.DomainConstraints {
		if strings.EqualFold(constraint.Name, name) {
			return constraint
		}
	}
	return nil
}

func (self *DataType) IdentityMatches(other *DataType) bool {
	if self == nil || other == nil {
		return false
	}

	return strings.EqualFold(self.Name, other.Name)
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

func (self *DataType) Equals(other *DataType) bool {
	if self == nil || other == nil {
		return false
	}

	if !self.Kind.Equals(other.Kind) {
		return false
	}

	// TODO(go,core) should we really consider identity to be part of equality given
	// things are allowed to be renamed, identity matching is usually done elsewhere,
	// and equality is usually only performed between two objects whose identity matches?
	// on the other hand, old dbsteward uses strict CREATE DDL equality instead of granular
	// matching, which includes the name
	if !strings.EqualFold(self.Name, other.Name) {
		return false
	}

	if self.Kind.Equals(DataTypeKindEnum) {
		if len(self.EnumValues) != len(other.EnumValues) {
			return false
		}
		// TODO(go,nth) this is not order-dependent, will cause unnecessary changes
		for i, selfVal := range self.EnumValues {
			if !selfVal.Equals(other.EnumValues[i]) {
				return false
			}
		}
		return true
	} else if self.Kind.Equals(DataTypeKindComposite) {
		if len(self.CompositeFields) != len(other.CompositeFields) {
			return false
		}
		for i, selfField := range self.CompositeFields {
			if !selfField.Equals(other.CompositeFields[i]) {
				return false
			}
		}
		return true
	} else if self.Kind.Equals(DataTypeKindDomain) {
		if !self.DomainType.Equals(other.DomainType) {
			return false
		}
		if len(self.DomainConstraints) != len(other.DomainConstraints) {
			return false
		}
		// TODO(go,nth) this is not order-dependent, will cause unnecessary changes
		for i, selfConstraint := range self.DomainConstraints {
			if !selfConstraint.Equals(other.DomainConstraints[i]) {
				return false
			}
		}
		return true
	} else {
		// TODO(go,nth) should we assert here or otherwise have some kind of warning?
		return false
	}
}

func (self *DataTypeEnumValue) Equals(other *DataTypeEnumValue) bool {
	if self == nil || other == nil {
		return false
	}
	// TODO(go,core) are non-postgres engines case insensitive?
	return self.Value == other.Value
}

func (self *DataTypeCompositeField) Equals(other *DataTypeCompositeField) bool {
	if self == nil || other == nil {
		return false
	}
	return strings.EqualFold(self.Name, other.Name) &&
		strings.EqualFold(self.Type, other.Type)
}

func (self *DataTypeDomainType) Equals(other *DataTypeDomainType) bool {
	if self == nil || other == nil {
		return false
	}
	return strings.EqualFold(self.BaseType, other.BaseType) &&
		self.Default == other.Default &&
		self.Nullable == other.Nullable
}

func (self *DataTypeDomainConstraint) GetNormalizedCheck() string {
	check := strings.TrimSpace(self.Check)
	if matches := util.IMatch(`^check\s*\((.*)\)$`, check); len(matches) > 1 {
		return strings.TrimSpace(matches[1])
	}
	return check
}

func (self *DataTypeDomainConstraint) Equals(other *DataTypeDomainConstraint) bool {
	if self == nil || other == nil {
		return false
	}
	return strings.EqualFold(self.Name, other.Name) &&
		self.GetNormalizedCheck() == other.GetNormalizedCheck()
}
