package sql

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/output"
)

type ColumnSetComment struct {
	Column  ColumnRef
	Comment string
}

func (self *ColumnSetComment) ToSql(q output.Quoter) string {
	return fmt.Sprintf(
		"COMMENT ON COLUMN %s IS %s;",
		self.Column.Qualified(q),
		q.LiteralString(self.Comment),
	)
}

type ColumnAlterStatistics struct {
	Column     ColumnRef
	Statistics int
}

func (self *ColumnAlterStatistics) ToSql(q output.Quoter) string {
	return NewTableAlter(*self.Column.TableRef(), &TableAlterPartColumnSetStatistics{
		Column:     self.Column.Column,
		Statistics: self.Statistics,
	}).ToSql(q)
}

type ColumnSetDefault struct {
	Column  ColumnRef
	Default ToSqlValue
}

func (self *ColumnSetDefault) ToSql(q output.Quoter) string {
	return NewTableAlter(*self.Column.TableRef(), &TableAlterPartColumnSetDefault{
		Column:  self.Column.Column,
		Default: self.Default,
	}).ToSql(q)
}

type ColumnSetNull struct {
	Column   ColumnRef
	Nullable bool
}

func (self *ColumnSetNull) ToSql(q output.Quoter) string {
	return NewTableAlter(*self.Column.TableRef(), &TableAlterPartColumnSetNull{
		Column:   self.Column.Column,
		Nullable: self.Nullable,
	}).ToSql(q)
}

type ColumnRename struct {
	Column  ColumnRef
	NewName string
}

func (self *ColumnRename) ToSql(q output.Quoter) string {
	return NewTableAlter(*self.Column.TableRef(), &TableAlterPartColumnRename{
		Column:  self.Column.Column,
		NewName: self.NewName,
	}).ToSql(q)
}

type ColumnDefinition struct {
	Name     string
	Type     TypeRef
	Default  ToSqlValue
	Nullable *bool
}

func (self *ColumnDefinition) GetSql(q output.Quoter) string {
	sql := q.QuoteColumn(self.Name) + " " + self.Type.Qualified(q)

	if self.Default != nil {
		sql += " DEFAULT " + self.Default.GetValueSql(q)
	}
	if self.Nullable != nil && !*self.Nullable {
		sql += " NOT NULL"
	}

	return sql
}
