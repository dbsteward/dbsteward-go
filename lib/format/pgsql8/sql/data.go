package sql

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

type DataInsert struct {
	Table   TableRef
	Columns []string
	Values  []string
}

func (self *DataInsert) ToSql(q output.Quoter) string {
	util.Assert(len(self.Columns) == len(self.Values), "len(cols) != len(vals)")
	cols := make([]string, len(self.Columns))
	for i, col := range self.Columns {
		cols[i] = q.QuoteColumn(col)
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
		self.Table.Qualified(q), strings.Join(cols, ", "), strings.Join(self.Values, ", "))
}

type DataUpdate struct {
	Table          TableRef
	UpdatedColumns []string
	UpdatedValues  []string
	KeyColumns     []string
	KeyValues      []string
}

func (self *DataUpdate) ToSql(q output.Quoter) string {
	util.Assert(len(self.UpdatedColumns) == len(self.UpdatedValues), "len(updated cols) != len(updated vals)")
	util.Assert(len(self.KeyColumns) == len(self.KeyValues), "len(key cols) != len(key vals)")

	updated := make([]string, len(self.UpdatedColumns))
	for i, upCol := range self.UpdatedColumns {
		updated[i] = fmt.Sprintf("%s = %s", q.QuoteColumn(upCol), self.UpdatedValues[i])
	}

	keys := make([]string, len(self.KeyColumns))
	for i, upCol := range self.KeyColumns {
		keys[i] = fmt.Sprintf("%s = %s", q.QuoteColumn(upCol), self.KeyValues[i])
	}

	return fmt.Sprintf(
		"UPDATE %s\nSET %s\nWHERE %s;",
		self.Table.Qualified(q),
		strings.Join(updated, ",\n    "),
		strings.Join(keys, "  AND "),
	)
}

type DataDelete struct {
	Table   TableRef
	Columns []string
	Values  []string
}

func (self *DataDelete) ToSql(q output.Quoter) string {
	util.Assert(len(self.Columns) == len(self.Values), "len(cols) != len(vals)")

	cols := make([]string, len(self.Columns))
	for i, col := range self.Columns {
		cols[i] = fmt.Sprintf("%s = %s", q.QuoteColumn(col), self.Values[i])
	}
	return fmt.Sprintf("DELETE FROM %s\nWHERE %s;", self.Table.Qualified(q), strings.Join(cols, "  AND "))
}
