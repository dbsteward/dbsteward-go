package sql

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

type TableAlterParts struct {
	Table TableRef
	Parts []TableAlterPart
}
type TableAlterPart interface {
	GetAlterPartSql(q output.Quoter) string
}

func NewTableAlter(table TableRef, parts ...TableAlterPart) *TableAlterParts {
	return &TableAlterParts{table, parts}
}

func (self *TableAlterParts) ToSql(q output.Quoter) string {
	parts := ""
	for _, part := range self.Parts {
		partSql := part.GetAlterPartSql(q)
		if partSql == "" {
			continue
		}
		if parts != "" {
			parts += ","
		}
		parts += "\n  " + partSql
	}
	if parts == "" {
		return ""
	}
	return fmt.Sprintf("ALTER TABLE %s%s;", self.Table.Qualified(q), parts)
}

type TableAlterPartAnnotation struct {
	Annotation string
	Wrapped    TableAlterPart
}

func (self *TableAlterPartAnnotation) GetAlterPartSql(q output.Quoter) string {
	// we use /* */ here instead of -- to avoid any issues with formatting subsequent
	// parts on the same line. indent the second line to match with TableAlterParts.ToSql
	return fmt.Sprintf("/* %s */\n  %s", strings.TrimSpace(self.Annotation), self.Wrapped.GetAlterPartSql(q))
}

type TableAlterPartOwner struct {
	Role string
}

func (self *TableAlterPartOwner) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("OWNER TO %s", q.QuoteRole(self.Role))
}

type TableAlterPartWithOids struct{}

func (self *TableAlterPartWithOids) GetAlterPartSql(output.Quoter) string {
	return "SET WITH OIDS"
}

type TableAlterPartWithoutOids struct{}

func (self *TableAlterPartWithoutOids) GetAlterPartSql(output.Quoter) string {
	return "SET WITHOUT OIDS"
}

type TableAlterPartSetStorageParams struct {
	Params map[string]string
}

func (self *TableAlterPartSetStorageParams) GetAlterPartSql(output.Quoter) string {
	if len(self.Params) == 0 {
		return ""
	}
	return fmt.Sprintf("SET (%s)", util.EncodeKV(self.Params, ",", "="))
}

type TableAlterPartResetStorageParams struct {
	Params []string
}

func (self *TableAlterPartResetStorageParams) GetAlterPartSql(output.Quoter) string {
	if len(self.Params) == 0 {
		return ""
	}
	return fmt.Sprintf("RESET (%s)", strings.Join(self.Params, ","))
}

type TableAlterPartSetTablespace struct {
	TablespaceName string
}

func (self *TableAlterPartSetTablespace) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("SET TABLESPACE %s", q.QuoteObject(self.TablespaceName))
}

type TableAlterPartRename struct {
	Name string
}

func (self *TableAlterPartRename) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("RENAME TO %s", q.QuoteTable(self.Name))
}

type TableAlterPartSetSchema struct {
	Name string
}

func (self *TableAlterPartSetSchema) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("SET SCHEMA %s", q.QuoteSchema(self.Name))
}

type TableAlterPartColumnSetDefault struct {
	Column  string
	Default ToSqlValue
}

func (self *TableAlterPartColumnSetDefault) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("ALTER COLUMN %s SET DEFAULT %s", self.Column, self.Default.GetValueSql(q))
}

type TableAlterPartColumnDropDefault struct {
	Column string
}

func (self *TableAlterPartColumnDropDefault) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("ALTER COLUMN %s DROP DEFAULT", q.QuoteColumn(self.Column))
}

type TableAlterPartColumnDrop struct {
	Column string
}

func (self *TableAlterPartColumnDrop) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("DROP COLUMN %s", q.QuoteColumn(self.Column))
}

type TableAlterPartColumnRename struct {
	Column  string
	NewName string
}

func (self *TableAlterPartColumnRename) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("RENAME COLUMN %s TO %s", q.QuoteColumn(self.Column), q.QuoteColumn(self.NewName))
}

type TableAlterPartColumnCreate struct {
	ColumnDef ColumnDefinition
}

func (self *TableAlterPartColumnCreate) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("ADD COLUMN %s", self.ColumnDef.GetSql(q))
}

type TableAlterPartColumnSetNull struct {
	Column   string
	Nullable bool
}

func (self *TableAlterPartColumnSetNull) GetAlterPartSql(q output.Quoter) string {
	op := "SET"
	if self.Nullable {
		op = "DROP"
	}
	return fmt.Sprintf("ALTER COLUMN %s %s NOT NULL", self.Column, op)
}

type TableAlterPartColumnSetStatistics struct {
	Column     string
	Statistics int
}

func (self *TableAlterPartColumnSetStatistics) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("ALTER COLUMN %s SET STATISTICS %d", self.Column, self.Statistics)
}

type TableAlterPartColumnChangeType struct {
	Column string
	Type   TypeRef
	Using  *ExpressionValue
}

func (self *TableAlterPartColumnChangeType) GetAlterPartSql(q output.Quoter) string {
	sql := fmt.Sprintf("ALTER COLUMN %s TYPE %s", q.QuoteColumn(self.Column), self.Type.Qualified(q))
	if self.Using != nil {
		sql += " USING " + self.Using.GetValueSql(q)
	}
	return sql
}

type TableAlterPartColumnChangeTypeUsingCast struct {
	Column string
	Type   TypeRef
}

func (self *TableAlterPartColumnChangeTypeUsingCast) GetAlterPartSql(q output.Quoter) string {
	expr := ExpressionValue(fmt.Sprintf("%s::%s", q.QuoteColumn(self.Column), self.Type.Qualified(q)))
	return (&TableAlterPartColumnChangeType{
		Column: self.Column,
		Type:   self.Type,
		Using:  &expr,
	}).GetAlterPartSql(q)
}
