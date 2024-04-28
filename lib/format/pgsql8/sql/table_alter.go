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

func (tap *TableAlterParts) ToSql(q output.Quoter) string {
	parts := ""
	for _, part := range tap.Parts {
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
	return fmt.Sprintf("ALTER TABLE %s%s;", tap.Table.Qualified(q), parts)
}

type TableAlterPartAnnotation struct {
	Annotation string
	Wrapped    TableAlterPart
}

func (tap *TableAlterPartAnnotation) GetAlterPartSql(q output.Quoter) string {
	// we use /* */ here instead of -- to avoid any issues with formatting subsequent
	// parts on the same line. indent the second line to match with TableAlterParts.ToSql
	return fmt.Sprintf("/* %s */\n  %s", strings.TrimSpace(tap.Annotation), tap.Wrapped.GetAlterPartSql(q))
}

type TableAlterPartOwner struct {
	Role string
}

func (owner *TableAlterPartOwner) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("OWNER TO %s", q.QuoteRole(owner.Role))
}

type TableAlterPartWithOids struct{}

func (t *TableAlterPartWithOids) GetAlterPartSql(output.Quoter) string {
	return "SET WITH OIDS"
}

type TableAlterPartWithoutOids struct{}

func (t *TableAlterPartWithoutOids) GetAlterPartSql(output.Quoter) string {
	return "SET WITHOUT OIDS"
}

type TableAlterPartSetStorageParams struct {
	Params map[string]string
}

func (t *TableAlterPartSetStorageParams) GetAlterPartSql(output.Quoter) string {
	if len(t.Params) == 0 {
		return ""
	}
	return fmt.Sprintf("SET (%s)", util.EncodeKV(t.Params, ",", "="))
}

type TableAlterPartResetStorageParams struct {
	Params []string
}

func (t *TableAlterPartResetStorageParams) GetAlterPartSql(output.Quoter) string {
	if len(t.Params) == 0 {
		return ""
	}
	return fmt.Sprintf("RESET (%s)", strings.Join(t.Params, ","))
}

type TableAlterPartSetTablespace struct {
	TablespaceName string
}

func (t *TableAlterPartSetTablespace) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("SET TABLESPACE %s", q.QuoteObject(t.TablespaceName))
}

type TableAlterPartRename struct {
	Name string
}

func (t *TableAlterPartRename) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("RENAME TO %s", q.QuoteTable(t.Name))
}

type TableAlterPartSetSchema struct {
	Name string
}

func (t *TableAlterPartSetSchema) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("SET SCHEMA %s", q.QuoteSchema(t.Name))
}

type TableAlterPartColumnSetDefault struct {
	Column  string
	Default ToSqlValue
}

func (t *TableAlterPartColumnSetDefault) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("ALTER COLUMN %s SET DEFAULT %s", q.QuoteColumn(t.Column), t.Default.GetValueSql(q))
}

type TableAlterPartColumnDropDefault struct {
	Column string
}

func (t *TableAlterPartColumnDropDefault) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("ALTER COLUMN %s DROP DEFAULT", q.QuoteColumn(t.Column))
}

type TableAlterPartColumnDrop struct {
	Column string
}

func (t *TableAlterPartColumnDrop) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("DROP COLUMN %s", q.QuoteColumn(t.Column))
}

type TableAlterPartColumnRename struct {
	Column  string
	NewName string
}

func (t *TableAlterPartColumnRename) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("RENAME COLUMN %s TO %s", q.QuoteColumn(t.Column), q.QuoteColumn(t.NewName))
}

type TableAlterPartColumnCreate struct {
	ColumnDef ColumnDefinition
}

func (t *TableAlterPartColumnCreate) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("ADD COLUMN %s", t.ColumnDef.GetSql(q))
}

type TableAlterPartColumnSetNull struct {
	Column   string
	Nullable bool
}

func (t *TableAlterPartColumnSetNull) GetAlterPartSql(q output.Quoter) string {
	op := "SET"
	if t.Nullable {
		op = "DROP"
	}
	return fmt.Sprintf("ALTER COLUMN %s %s NOT NULL", q.QuoteColumn(t.Column), op)
}

type TableAlterPartColumnSetStatistics struct {
	Column     string
	Statistics int
}

func (t *TableAlterPartColumnSetStatistics) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("ALTER COLUMN %s SET STATISTICS %d", q.QuoteColumn(t.Column), t.Statistics)
}

type TableAlterPartColumnChangeType struct {
	Column string
	Type   TypeRef
	Using  *ExpressionValue
}

func (t *TableAlterPartColumnChangeType) GetAlterPartSql(q output.Quoter) string {
	sql := fmt.Sprintf("ALTER COLUMN %s TYPE %s", q.QuoteColumn(t.Column), t.Type.Qualified(q))
	if t.Using != nil {
		sql += " USING " + t.Using.GetValueSql(q)
	}
	return sql
}

type TableAlterPartColumnChangeTypeUsingCast struct {
	Column string
	Type   TypeRef
}

func (t *TableAlterPartColumnChangeTypeUsingCast) GetAlterPartSql(q output.Quoter) string {
	expr := ExpressionValue(fmt.Sprintf("%s::%s", q.QuoteColumn(t.Column), t.Type.Qualified(q)))
	return (&TableAlterPartColumnChangeType{
		Column: t.Column,
		Type:   t.Type,
		Using:  &expr,
	}).GetAlterPartSql(q)
}

type TableAlterPartSetWithoutCluster struct{}

func (t *TableAlterPartSetWithoutCluster) GetAlterPartSql(q output.Quoter) string {
	return "SET WITHOUT CLUSTER"
}

type TableAlterPartClusterOn struct {
	Index string
}

func (t *TableAlterPartClusterOn) GetAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("CLUSTER ON %s", q.QuoteObject(t.Index))
}
