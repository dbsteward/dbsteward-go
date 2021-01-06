package sql

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/output"
)

type SequenceSetValSerialMax struct {
	Schema string
	Table  string
	Column string
}

func (self *SequenceSetValSerialMax) ToSql(q output.Quoter) string {
	// TODO(go,core) quoting
	return fmt.Sprintf(
		`SELECT setval(pg_get_serial_sequence(%s, %s), MAX(%s), true) FROM %s;`,
		q.LiteralString(self.Schema+"."+self.Table), // TODO(go,nth) find out how a quoted identifier would work here
		q.LiteralString(self.Column),
		q.QuoteColumn(self.Column),
		q.QualifyTable(self.Schema, self.Table),
	)
}

type SequenceGrant struct {
	Sequence SequenceRef
	Perms    []string
	Roles    []string
	CanGrant bool
}

func (self *SequenceGrant) ToSql(q output.Quoter) string {
	return (&grant{
		grantTypeSequence,
		&self.Sequence,
		self.Perms,
		self.Roles,
		self.CanGrant,
	}).ToSql(q)
}
