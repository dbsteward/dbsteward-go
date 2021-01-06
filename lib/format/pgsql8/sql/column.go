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
