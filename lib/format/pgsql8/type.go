package pgsql8

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

type DataType struct {
}

func NewDataType() *DataType {
	return &DataType{}
}

func (self *DataType) GetCreationSql(schema *model.Schema, datatype *model.DataType) []output.ToSql {
	switch datatype.Kind {
	case model.DataTypeKindEnum:
		// TODO(go,3) put validation elsewhere
		if len(datatype.EnumValues) == 0 {
			lib.GlobalDBSteward.Fatal("Enum type %s.%s contains no enum children", schema.Name, datatype.Name)
		}
		return []output.ToSql{
			&sql.TypeEnumCreate{
				Type:   sql.TypeRef{schema.Name, datatype.Name},
				Values: datatype.EnumValues,
			},
		}
	case model.DataTypeKindComposite:
		// TODO(go,3) put validation elsewhere
		if len(datatype.CompositeFields) == 0 {
			lib.GlobalDBSteward.Fatal("Composite type %s.%s contains no typeCompositeElement children", schema.Name, datatype.Name)
		}
		fields := make([]sql.TypeCompositeCreateField, len(datatype.CompositeFields))
		for i, field := range datatype.CompositeFields {
			fields[i] = sql.TypeCompositeCreateField{
				Name: field.Name,
				Type: field.Type,
			}
		}
		return []output.ToSql{
			&sql.TypeCompositeCreate{
				Type:   sql.TypeRef{schema.Name, datatype.Name},
				Fields: fields,
			},
		}
	case model.DataTypeKindDomain:
		// TODO(go,3) put validation elsewhere
		if datatype.DomainType == nil {
			lib.GlobalDBSteward.Fatal("Domain type %s.%s contains no domainType child", schema.Name, datatype.Name)
		}
		if datatype.DomainType.BaseType == "" {
			lib.GlobalDBSteward.Fatal("Domain type %s.%s baseType attribute is not set on domainType", schema.Name, datatype.Name)
		}
		constraints := make([]sql.TypeDomainCreateConstraint, len(datatype.DomainConstraints))
		for i, constraint := range datatype.DomainConstraints {
			// TODO(go,3) put normalization elsewhere
			name := strings.TrimSpace(constraint.Name)
			check := strings.TrimSpace(constraint.Check)
			if name == "" {
				lib.GlobalDBSteward.Fatal("Domain type %s.%s constraint %d has empty name", schema.Name, datatype.Name, i)
			}
			if check == "" {
				lib.GlobalDBSteward.Fatal("Domain type %s.%s constraint %s has no definition", schema.Name, datatype.Name, name)
			}
			if util.IHasPrefix(check, "check(") {
				check = check[len("check(") : len(check)-1]
			}
			constraints[i] = sql.TypeDomainCreateConstraint{
				Name:  name,
				Check: check,
			}
		}

		return []output.ToSql{
			&sql.TypeDomainCreate{
				Type:        sql.TypeRef{schema.Name, datatype.Name},
				BaseType:    strings.TrimSpace(datatype.DomainType.BaseType),
				Default:     strings.TrimSpace(datatype.DomainType.Default),
				Nullable:    datatype.DomainType.Nullable,
				Constraints: constraints,
			},
		}
	}
	lib.GlobalDBSteward.Fatal("Unknown type %s type %s", datatype.Name, datatype.Kind)
	return nil
}

func (self *DataType) IsLinkedTableType(spec string) bool {
	// TODO(go,pgsql) see pgsql8::PATTERN_TABLE_LINKED_TYPES
	return false
}
