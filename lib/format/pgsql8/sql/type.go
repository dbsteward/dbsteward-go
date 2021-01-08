package sql

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/output"
)

type TypeEnumCreate struct {
	Type   TypeRef
	Values []string
}

func (self *TypeEnumCreate) ToSql(q output.Quoter) string {
	values := make([]string, len(self.Values))
	for i, value := range self.Values {
		values[i] = q.LiteralString(value)
	}
	return fmt.Sprintf("CREATE TYPE %s AS ENUM (%s);", self.Type.Qualified(q), strings.Join(values, ", "))
}

type TypeCompositeCreate struct {
	Type   TypeRef
	Fields []TypeCompositeCreateField
}
type TypeCompositeCreateField struct {
	Name string
	Type string
}

func (self *TypeCompositeCreate) ToSql(q output.Quoter) string {
	fields := make([]string, len(self.Fields))
	for i, field := range self.Fields {
		fields[i] = fmt.Sprintf("%s %s", field.Name, field.Type)
	}
	return fmt.Sprintf("CREATE TYPE %s AS (\n  %s\n);", self.Type.Qualified(q), strings.Join(fields, ",\n  "))
}

type TypeDomainCreate struct {
	Type        TypeRef
	BaseType    string
	Default     string
	Nullable    bool
	Constraints []TypeDomainCreateConstraint
}
type TypeDomainCreateConstraint struct {
	Name  string
	Check string
}

func (self *TypeDomainCreate) ToSql(q output.Quoter) string {
	// TODO(feat) quote the basetype?
	ddl := fmt.Sprintf("CREATE DOMAIN %s AS %s", self.Type.Qualified(q), self.BaseType)
	if self.Default != "" {
		ddl += "\n  DEFAULT " + q.LiteralValue(self.BaseType, self.Default)
	}
	if !self.Nullable {
		ddl += "\n  NOT NULL"
	}
	for _, constraint := range self.Constraints {
		ddl += fmt.Sprintf("\n  CONSTRAINT %s CHECK(%s)", q.QuoteObject(constraint.Name), constraint.Check)
	}
	return ddl + ";"
}
