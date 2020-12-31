package sql

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/output"
)

type SetValSerialSequenceMax struct {
	Schema string
	Table  string
	Column string
}

func (self *SetValSerialSequenceMax) ToSql(q output.Quoter) string {
	// TODO(go,core) quoting
	return fmt.Sprintf(
		`SELECT setval(pg_get_serial_sequence(%s, %s), MAX(%s), true) FROM %s;`,
		q.LiteralString(self.Schema+"."+self.Table), // TODO(go,nth) find out how a quoted identifier would work here
		q.LiteralString(self.Column),
		q.QuoteColumn(self.Column),
		q.QualifyTable(self.Schema, self.Table),
	)
}
