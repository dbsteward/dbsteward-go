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

type TypeDrop struct {
	Type TypeRef
}

func (self *TypeDrop) ToSql(q output.Quoter) string {
	return fmt.Sprintf("DROP TYPE %s;", self.Type.Qualified(q))
}

type TypeDomainCreate struct {
	Type        TypeRef
	BaseType    string
	Default     ToSqlValue
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
	if self.Default != nil {
		ddl += "\n  DEFAULT " + self.Default.GetValueSql(q)
	}
	if !self.Nullable {
		ddl += "\n  NOT NULL"
	}
	for _, constraint := range self.Constraints {
		ddl += fmt.Sprintf("\n  CONSTRAINT %s CHECK(%s)", q.QuoteObject(constraint.Name), constraint.Check)
	}
	return ddl + ";"
}

type TypeDomainDrop struct {
	Type TypeRef
}

func (self *TypeDomainDrop) ToSql(q output.Quoter) string {
	return fmt.Sprintf("DROP DOMAIN %s;", self.Type.Qualified(q))
}

type TypeDomainAlterDropDefault struct {
	Type TypeRef
}

func (self *TypeDomainAlterDropDefault) ToSql(q output.Quoter) string {
	return fmt.Sprintf("ALTER DOMAIN %s DROP DEFAULT;", self.Type.Qualified(q))
}

type TypeDomainAlterSetDefault struct {
	Type  TypeRef
	Value ToSqlValue
}

func (self *TypeDomainAlterSetDefault) ToSql(q output.Quoter) string {
	return fmt.Sprintf("ALTER DOMAIN %s SET DEFAULT %s;", self.Type.Qualified(q), self.Value.GetValueSql(q))
}

type TypeDomainAlterSetNullable struct {
	Type     TypeRef
	Nullable bool
}

func (self *TypeDomainAlterSetNullable) ToSql(q output.Quoter) string {
	op := "SET"
	if self.Nullable {
		op = "DROP"
	}
	return fmt.Sprintf("ALTER DOMAIN %s %s NOT NULL;", self.Type.Qualified(q), op)
}

type TypeDomainAlterDropConstraint struct {
	Type       TypeRef
	Constraint string
}

func (self *TypeDomainAlterDropConstraint) ToSql(q output.Quoter) string {
	return fmt.Sprintf("ALTER DOMAIN %s DROP CONSTRAINT %s;", self.Type.Qualified(q), q.QuoteObject(self.Constraint))
}

type TypeDomainAlterAddConstraint struct {
	Type       TypeRef
	Constraint string
	Check      ToSqlValue
}

func (self *TypeDomainAlterAddConstraint) ToSql(q output.Quoter) string {
	return fmt.Sprintf("ALTER DOMAIN %s ADD CONSTRAINT %s CHECK(%s);", self.Type.Qualified(q), q.QuoteObject(self.Constraint), self.Check.GetValueSql(q))
}
