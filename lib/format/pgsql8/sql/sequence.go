package sql

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/output"
)

type SequenceCreate struct {
	Sequence  SequenceRef
	Cache     *int
	Start     *int
	Min       *int
	Max       *int
	Increment *int
	Cycle     bool
	OwnedBy   string
}

func (self *SequenceCreate) ToSql(q output.Quoter) string {
	ddl := "CREATE SEQUENCE " + self.Sequence.Qualified(q)
	if self.Increment != nil {
		ddl += fmt.Sprintf("\n  INCREMENT BY %d", *self.Increment)
	}
	if self.Min != nil {
		ddl += fmt.Sprintf("\n  MINVALUE %d", *self.Min)
	}
	if self.Max != nil {
		ddl += fmt.Sprintf("\n  MAXVALUE %d", *self.Max)
	}
	if self.Start != nil {
		ddl += fmt.Sprintf("\n  START WITH %d", *self.Start)
	}
	if self.Cache != nil {
		ddl += fmt.Sprintf("\n  CACHE %d", *self.Cache)
	}
	if self.Cycle {
		ddl += "\n  CYCLE"
	}
	if self.OwnedBy != "" {
		ddl += "\n  OWNED BY " + self.OwnedBy
	}
	return ddl + ";"
}

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

type SequenceAlterOwner struct {
	Sequence SequenceRef
	Role     string
}

func (self *SequenceAlterOwner) ToSql(q output.Quoter) string {
	return fmt.Sprintf(
		"ALTER SEQUENCE %s OWNER TO %s;",
		self.Sequence.Qualified(q),
		q.QuoteRole(self.Role),
	)
}

type SequenceSetComment struct {
	Sequence SequenceRef
	Comment  string
}

func (self *SequenceSetComment) ToSql(q output.Quoter) string {
	return fmt.Sprintf(
		"COMMENT ON SEQUENCE %s IS %s;",
		self.Sequence.Qualified(q),
		q.LiteralString(self.Comment),
	)
}
