package sql

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/output"
)

type ColumnRef struct {
	Schema string
	Table  string
	Column string
}

func (self *ColumnRef) Qualified(q output.Quoter) string {
	return q.QualifyColumn(self.Schema, self.Table, self.Column)
}

func (self *ColumnRef) QualifiedTable(q output.Quoter) string {
	return q.QualifyTable(self.Schema, self.Table)
}

func (self *ColumnRef) Quoted(q output.Quoter) string {
	return q.QuoteColumn(self.Column)
}

type SetColumnComment struct {
	Column  ColumnRef
	Comment string
}

func (self *SetColumnComment) ToSql(q output.Quoter) string {
	return fmt.Sprintf(
		"COMMENT ON COLUMN %s IS %s;\n",
		self.Column.Qualified(q),
		q.LiteralString(self.Comment),
	)
}

type AlterColumnStatistics struct {
	Column     ColumnRef
	Statistics int
}

func (self *AlterColumnStatistics) ToSql(q output.Quoter) string {
	return fmt.Sprintf(
		"ALTER TABLE ONLY %s ALTER COLUMN %s SET STATISTICS %d",
		self.Column.QualifiedTable(q),
		self.Column.Quoted(q),
		self.Statistics,
	)
}
