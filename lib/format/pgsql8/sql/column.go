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
	return fmt.Sprintf(
		"ALTER TABLE ONLY %s ALTER COLUMN %s SET STATISTICS %d;",
		self.Column.QualifiedTable(q),
		self.Column.Quoted(q),
		self.Statistics,
	)
}

type ColumnSetDefault struct {
	Column  ColumnRef
	Default string
}

func (self *ColumnSetDefault) ToSql(q output.Quoter) string {
	// TODO(feat) handle default quoting?
	return fmt.Sprintf(
		"ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;",
		self.Column.QualifiedTable(q),
		self.Column.Quoted(q),
		self.Default,
	)
}
