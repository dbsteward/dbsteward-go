package ir

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
)

type TypeDefKind int

const (
	DataTypeKindEnum TypeDefKind = iota
	DataTypeKindComposite
	DataTypeKindDomain
)

// String returns a value suitable for showing the user.
// !! Do not use this as part of SQL. It can change and
// is not intended to be valid SQL!!
func (tdk TypeDefKind) String() string {
	switch tdk {
	case DataTypeKindEnum:
		return "enum"
	case DataTypeKindComposite:
		return "composite"
	case DataTypeKindDomain:
		return "domain"
	default:
		return "unknown"
	}
}

// TypeDef is a custom type definition
type TypeDef struct {
	Name              string
	Kind              TypeDefKind
	EnumValues        []DataTypeEnumValue
	CompositeFields   []DataTypeCompositeField
	DomainType        *DataTypeDomainType
	DomainConstraints []DataTypeDomainConstraint
}

type DataTypeEnumValue string

type DataTypeCompositeField struct {
	Name string
	Type string
}

type DataTypeDomainType struct {
	BaseType string
	Default  string
	Nullable bool
}

type DataTypeDomainConstraint struct {
	Name  string
	Check string
}

func (td *TypeDef) TryGetDomainConstraintNamed(name string) *DataTypeDomainConstraint {
	util.Assert(td.Kind == DataTypeKindDomain, "can only be called for Domain kind")
	for _, constraint := range td.DomainConstraints {
		if strings.EqualFold(constraint.Name, name) {
			return &constraint
		}
	}
	return nil
}

func (td *TypeDef) IdentityMatches(other *TypeDef) bool {
	if td == nil || other == nil {
		return false
	}

	return strings.EqualFold(td.Name, other.Name)
}

func (td *TypeDef) Merge(overlay *TypeDef) {
	if overlay == nil {
		return
	}
	td.Kind = overlay.Kind
	td.EnumValues = overlay.EnumValues
	td.CompositeFields = overlay.CompositeFields
	td.DomainType = overlay.DomainType
	td.DomainConstraints = overlay.DomainConstraints
}

func (td *TypeDef) Validate(doc *Definition, schema *Schema) []error {
	out := []error{}

	// note: this used to be handled at DTD validation stage, but that doesn't guard against
	// manually constructed representations or alternately parsed representations

	// TODO(go,3) is there a better way to translate these errors back to something indicative of what the user
	// should do for a given representation e.g. "must have a <domainType> element"? how do we have both agnostic
	// validation AND format-specific errors?
	// TODO(go,3) we should probably look into making these cases unrepresentable at all;
	//   will require splitting marshalling structs from model structs (which is good!)
	// TODO(go,3) further validate the component structs

	switch td.Kind {
	case DataTypeKindEnum:
		if len(td.EnumValues) == 0 {
			out = append(out, fmt.Errorf("enum data type %s.%s must define at least one enum value", schema.Name, td.Name))
		}
		if len(td.DomainConstraints) > 0 {
			out = append(out, fmt.Errorf("enum data type %s.%s must not define a domain constraint", schema.Name, td.Name))
		}
		if td.DomainType != nil {
			out = append(out, fmt.Errorf("enum data type %s.%s must not define a domain type", schema.Name, td.Name))
		}
		if len(td.CompositeFields) > 0 {
			out = append(out, fmt.Errorf("enum data type %s.%s must not define composite fields", schema.Name, td.Name))
		}
	case DataTypeKindDomain:
		if len(td.EnumValues) > 0 {
			out = append(out, fmt.Errorf("domain data type %s.%s must not define enum values", schema.Name, td.Name))
		}
		if td.DomainType == nil {
			out = append(out, fmt.Errorf("domain data type %s.%s must define a domain type", schema.Name, td.Name))
		}
		if len(td.CompositeFields) > 0 {
			out = append(out, fmt.Errorf("domain data type %s.%s must not define composite fields", schema.Name, td.Name))
		}
	case DataTypeKindComposite:
		if len(td.EnumValues) > 0 {
			out = append(out, fmt.Errorf("composite data type %s.%s must not define enum values", schema.Name, td.Name))
		}
		if len(td.DomainConstraints) > 0 {
			out = append(out, fmt.Errorf("composite data type %s.%s must not define a domain constraint", schema.Name, td.Name))
		}
		if td.DomainType != nil {
			out = append(out, fmt.Errorf("composite data type %s.%s must not define a domain type", schema.Name, td.Name))
		}
		if len(td.CompositeFields) == 0 {
			out = append(out, fmt.Errorf("composite data type %s.%s must define at least one composite field", schema.Name, td.Name))
		}
	}

	return out
}

func (td *TypeDef) Equals(other *TypeDef) bool {
	if td == nil || other == nil {
		return false
	}

	if td.Kind != other.Kind {
		return false
	}

	// TODO(go,core) should we really consider identity to be part of equality given
	// things are allowed to be renamed, identity matching is usually done elsewhere,
	// and equality is usually only performed between two objects whose identity matches?
	// on the other hand, old dbsteward uses strict CREATE DDL equality instead of granular
	// matching, which includes the name
	if !strings.EqualFold(td.Name, other.Name) {
		return false
	}

	if td.Kind == DataTypeKindEnum {
		if len(td.EnumValues) != len(other.EnumValues) {
			return false
		}
		// TODO(go,nth) this is not order-dependent, will cause unnecessary changes
		for i, selfVal := range td.EnumValues {
			if !selfVal.Equals(other.EnumValues[i]) {
				return false
			}
		}
		return true
	} else if td.Kind == DataTypeKindComposite {
		if len(td.CompositeFields) != len(other.CompositeFields) {
			return false
		}
		for i, selfField := range td.CompositeFields {
			if !selfField.Equals(other.CompositeFields[i]) {
				return false
			}
		}
		return true
	} else if td.Kind == DataTypeKindDomain {
		if !td.DomainType.Equals(other.DomainType) {
			return false
		}
		if len(td.DomainConstraints) != len(other.DomainConstraints) {
			return false
		}
		// TODO(go,nth) this is not order-dependent, will cause unnecessary changes
		for i, selfConstraint := range td.DomainConstraints {
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

func (enumVal DataTypeEnumValue) Equals(other DataTypeEnumValue) bool {
	// TODO(go,core) are non-postgres engines case insensitive?
	return enumVal == other
}

func (dtcf DataTypeCompositeField) Equals(other DataTypeCompositeField) bool {
	return strings.EqualFold(dtcf.Name, other.Name) &&
		strings.EqualFold(dtcf.Type, other.Type)
}

func (domain *DataTypeDomainType) Equals(other *DataTypeDomainType) bool {
	if domain == nil || other == nil {
		return false
	}
	return strings.EqualFold(domain.BaseType, other.BaseType) &&
		domain.Default == other.Default &&
		domain.Nullable == other.Nullable
}

func (dConst *DataTypeDomainConstraint) GetNormalizedCheck() string {
	// @TODO: This is Postgres-specific and doesnt't belong in the IR.
	// However, it's assumed by .Equals() so will require some careful
	// work to remove it.
	check := strings.TrimSpace(dConst.Check)
	if matches := util.IMatch(`^check\s*\((.*)\)$`, check); len(matches) > 1 {
		return strings.TrimSpace(matches[1])
	}
	return check
}

func (dConst DataTypeDomainConstraint) Equals(other DataTypeDomainConstraint) bool {
	return strings.EqualFold(dConst.Name, other.Name) &&
		dConst.GetNormalizedCheck() == other.GetNormalizedCheck()
}
