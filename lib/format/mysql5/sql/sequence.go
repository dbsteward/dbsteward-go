package sql

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/output"
)

func getSerialSequenceName(schema, table, column string) string {
	return fmt.Sprintf("__%s_%s_%s_serial_seq", schema, table, column)
}

type SequenceSerialSetValMax struct {
	Column ColumnRef
}

func (self *SequenceSerialSetValMax) ToSql(q output.Quoter) string {
	return (&SequenceSetVal{
		Sequence:      getSerialSequenceName(self.Column.Schema, self.Column.Table, self.Column.Column),
		Value:         RawSql(fmt.Sprintf("MAX(%s)", self.Column.Quoted(q))),
		ShouldAdvance: false,
		FromTable:     self.Column.TableRef(),
	}).ToSql(q)
}

// implements `SELECT setval(...);`
// see https://www.postgresql.org/docs/13/functions-sequence.html
type SequenceSetVal struct {
	Sequence      string
	Value         ToSqlValue
	ShouldAdvance bool
	FromTable     *TableRef
}

func (self *SequenceSetVal) ToSql(q output.Quoter) string {
	from := ""
	if self.FromTable != nil {
		from = " FROM " + self.FromTable.Qualified(q)
	}
	return fmt.Sprintf("SELECT setval(%s, %s, %t)%s;", q.LiteralString(self.Sequence), self.Value.GetValueSql(q), self.ShouldAdvance, from)
}
