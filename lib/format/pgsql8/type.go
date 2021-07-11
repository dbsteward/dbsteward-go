package pgsql8

import (
	"fmt"
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

func (self *DataType) GetCreationSql(schema *model.Schema, datatype *model.DataType) ([]output.ToSql, error) {
	switch datatype.Kind {
	case model.DataTypeKindEnum:
		// TODO(go,3) put validation elsewhere
		if len(datatype.EnumValues) == 0 {
			return nil, fmt.Errorf("Enum type %s.%s contains no enum children", schema.Name, datatype.Name)
		}
		vals := make([]string, len(datatype.EnumValues))
		for i, val := range datatype.EnumValues {
			vals[i] = val.Value
		}
		return []output.ToSql{
			&sql.TypeEnumCreate{
				Type:   sql.TypeRef{schema.Name, datatype.Name},
				Values: vals,
			},
		}, nil
	case model.DataTypeKindComposite:
		// TODO(go,3) put validation elsewhere
		if len(datatype.CompositeFields) == 0 {
			return nil, fmt.Errorf("Composite type %s.%s contains no typeCompositeElement children", schema.Name, datatype.Name)
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
		}, nil
	case model.DataTypeKindDomain:
		// TODO(go,3) put validation elsewhere
		if datatype.DomainType == nil {
			return nil, fmt.Errorf("Domain type %s.%s contains no domainType child", schema.Name, datatype.Name)
		}
		if datatype.DomainType.BaseType == "" {
			return nil, fmt.Errorf("Domain type %s.%s baseType attribute is not set on domainType", schema.Name, datatype.Name)
		}
		constraints := make([]sql.TypeDomainCreateConstraint, len(datatype.DomainConstraints))
		for i, constraint := range datatype.DomainConstraints {
			// TODO(go,3) put normalization elsewhere
			name := strings.TrimSpace(constraint.Name)
			check := strings.TrimSpace(constraint.GetNormalizedCheck())
			if name == "" {
				return nil, fmt.Errorf("Domain type %s.%s constraint %d has empty name", schema.Name, datatype.Name, i)
			}
			if check == "" {
				return nil, fmt.Errorf("Domain type %s.%s constraint %s has no definition", schema.Name, datatype.Name, name)
			}
			constraints[i] = sql.TypeDomainCreateConstraint{
				Name:  name,
				Check: check,
			}
		}

		// TODO(feat) how do we distinguish between DEFAULT '' and no default?
		var def sql.ToSqlValue
		if datatype.DomainType.Default != "" {
			def = sql.NewValue(datatype.DomainType.BaseType, datatype.DomainType.Default, datatype.DomainType.Nullable)
		}

		return []output.ToSql{
			&sql.TypeDomainCreate{
				Type:        sql.TypeRef{schema.Name, datatype.Name},
				BaseType:    datatype.DomainType.BaseType,
				Default:     def,
				Nullable:    datatype.DomainType.Nullable,
				Constraints: constraints,
			},
		}, nil
	}
	return nil, fmt.Errorf("Unknown type %s type %s", datatype.Name, datatype.Kind)
}

func (self *DataType) GetDropSql(schema *model.Schema, datatype *model.DataType) []output.ToSql {
	if datatype.Kind.Equals(model.DataTypeKindDomain) {
		return []output.ToSql{
			&sql.TypeDomainDrop{
				Type: sql.TypeRef{schema.Name, datatype.Name},
			},
		}
	}
	return []output.ToSql{
		&sql.TypeDrop{
			Type: sql.TypeRef{schema.Name, datatype.Name},
		},
	}
}

func (self *DataType) IsLinkedTableType(spec string) bool {
	// TODO(go,pgsql) unify these
	return self.IsSerialType(spec)
}

func (self *DataType) IsSerialType(spec string) bool {
	return strings.EqualFold(spec, DataTypeSerial) || strings.EqualFold(spec, DataTypeBigSerial)
}

func (self *DataType) IsIntType(spec string) bool {
	return util.IIndex(spec, "int") >= 0
}

// Change all table columns that are the given datatype to a placeholder type
func (self *DataType) AlterColumnTypePlaceholder(schema *model.Schema, datatype *model.DataType) ([]*model.ColumnRef, []output.ToSql) {
	ddl := []output.ToSql{}
	cols := []*model.ColumnRef{}
	for _, newTableRef := range GlobalDiff.NewTableDependency {
		for _, newColumn := range newTableRef.Table.Columns {
			columnType := GlobalColumn.GetColumnType(lib.GlobalDBSteward.NewDatabase, newTableRef.Schema, newTableRef.Table, newColumn)
			if strings.EqualFold(columnType, datatype.Name) || strings.EqualFold(columnType, newTableRef.Schema.Name+"."+datatype.Name) {
				sqlRef := sql.TableRef{newTableRef.Schema.Name, newTableRef.Table.Name}
				ddl = append(ddl, sql.NewTableAlter(sqlRef, &sql.TableAlterPartColumnChangeType{
					Column: newColumn.Name,
					Type:   self.alterColumnTypePlaceholderType(datatype),
				}))
				cols = append(cols, newTableRef.ToColumnRef(newColumn))
			}
		}
	}
	return cols, ddl
}

func (self *DataType) alterColumnTypePlaceholderType(datatype *model.DataType) sql.TypeRef {
	if datatype.Kind.Equals(model.DataTypeKindEnum) {
		return sql.BuiltinTypeRef("text")
	}
	if datatype.Kind.Equals(model.DataTypeKindDomain) {
		return sql.ParseTypeRef(datatype.DomainType.BaseType)
	}
	util.Assert(false, "Unexpected data type kind %s", string(datatype.Kind))
	return sql.TypeRef{} // unreachable
}

// restores types changed by AlterColumnTypePlaceholder
func (self *DataType) AlterColumnTypeRestore(columns []*model.ColumnRef, schema *model.Schema, datatype *model.DataType) []output.ToSql {
	ddl := []output.ToSql{}
	// do the columns backwards to maintain dependency ordering
	for i := len(columns) - 1; i >= 0; i-- {
		sqlRef := sql.TableRef{columns[i].Schema.Name, columns[i].Table.Name}
		ddl = append(ddl, sql.NewTableAlter(sqlRef, &sql.TableAlterPartColumnChangeTypeUsingCast{
			Column: columns[i].Column.Name,
			Type:   sql.TypeRef{schema.Name, datatype.Name},
		}))
	}
	return ddl
}
