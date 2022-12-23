package sql

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

type SequenceCreate struct {
	Sequence  SequenceRef
	Cache     util.Opt[int]
	Start     util.Opt[int]
	Min       util.Opt[int]
	Max       util.Opt[int]
	Increment util.Opt[int]
	Cycle     bool
	OwnedBy   string
}

func (self *SequenceCreate) ToSql(q output.Quoter) string {
	ddl := "CREATE SEQUENCE " + self.Sequence.Qualified(q)
	if val, ok := self.Increment.Maybe(); ok {
		ddl += fmt.Sprintf("\n  INCREMENT BY %d", val)
	}
	if val, ok := self.Min.Maybe(); ok {
		ddl += fmt.Sprintf("\n  MINVALUE %d", val)
	} else {
		// NOTE: this is technically not needed, NO MINVALUE is the default
		ddl += "\n  NO MINVALUE"
	}
	if val, ok := self.Max.Maybe(); ok {
		ddl += fmt.Sprintf("\n  MAXVALUE %d", val)
	} else {
		// NOTE: this is technically not needed, NO MINVALUE is the default
		ddl += "\n  NO MAXVALUE"
	}
	if val, ok := self.Start.Maybe(); ok {
		ddl += fmt.Sprintf("\n  START WITH %d", val)
	}
	if val, ok := self.Cache.Maybe(); ok {
		ddl += fmt.Sprintf("\n  CACHE %d", val)
	}
	if self.Cycle {
		ddl += "\n  CYCLE"
	}
	if self.OwnedBy != "" {
		ddl += "\n  OWNED BY " + self.OwnedBy
	}
	return ddl + ";"
}

type SequenceDrop struct {
	Sequence SequenceRef
}

func (self *SequenceDrop) ToSql(q output.Quoter) string {
	// TODO(feat) if exists?
	return fmt.Sprintf("DROP SEQUENCE IF EXISTS %s;", self.Sequence.Qualified(q))
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

type SequenceSerialSetValMax struct {
	Column ColumnRef
}

func (self *SequenceSerialSetValMax) ToSql(q output.Quoter) string {
	return (&SequenceSetVal{
		Sequence:  &SequenceGetSerialName{self.Column},
		Value:     RawSql(fmt.Sprintf("MAX(%s)", self.Column.Quoted(q))),
		IsCalled:  true,
		FromTable: self.Column.TableRef(),
	}).ToSql(q)
}

type SequenceSerialSetVal struct {
	Column ColumnRef
	Value  int
}

func (self *SequenceSerialSetVal) ToSql(q output.Quoter) string {
	return (&SequenceSetVal{
		Sequence: &SequenceGetSerialName{
			Column: self.Column,
		},
		Value:    IntValue(self.Value),
		IsCalled: true,
	}).ToSql(q)
}

// implements `SELECT setval(...);`
// see https://www.postgresql.org/docs/13/functions-sequence.html
type SequenceSetVal struct {
	Sequence  ToSqlValue
	Value     ToSqlValue
	IsCalled  bool
	FromTable *TableRef
}

func (self *SequenceSetVal) ToSql(q output.Quoter) string {
	from := ""
	if self.FromTable != nil {
		from = " FROM " + self.FromTable.Qualified(q)
	}
	return fmt.Sprintf("SELECT setval(%s, %s, %t)%s;", self.Sequence.GetValueSql(q), self.Value.GetValueSql(q), self.IsCalled, from)
}

// implements `pg_get_serial_sequence(...)`
type SequenceGetSerialName struct {
	Column ColumnRef
}

func (self *SequenceGetSerialName) GetValueSql(q output.Quoter) string {
	return fmt.Sprintf("pg_get_serial_sequence(%s, %s)", self.Column.TableRef().QualifiedLiteralString(q), q.LiteralString(self.Column.Column))
}
