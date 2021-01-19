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
	Values  []ToSqlValue
}

func (self *DataInsert) ToSql(q output.Quoter) string {
	util.Assert(len(self.Columns) == len(self.Values), "len(cols) != len(vals)")
	if len(self.Columns) == 0 {
		return ""
	}

	cols := make([]string, len(self.Columns))
	vals := make([]string, len(self.Values))
	for i, col := range self.Columns {
		cols[i] = q.QuoteColumn(col)
		vals[i] = self.Values[i].GetValueSql(q)
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
		self.Table.Qualified(q), strings.Join(cols, ", "), strings.Join(vals, ", "))
}

type DataUpdate struct {
	Table          TableRef
	UpdatedColumns []string
	UpdatedValues  []ToSqlValue
	KeyColumns     []string
	KeyValues      []ToSqlValue
}

func (self *DataUpdate) ToSql(q output.Quoter) string {
	util.Assert(len(self.UpdatedColumns) == len(self.UpdatedValues), "len(updated cols) != len(updated vals)")
	util.Assert(len(self.KeyColumns) == len(self.KeyValues), "len(key cols) != len(key vals)")

	if len(self.UpdatedColumns) == 0 {
		return ""
	}

	updated := make([]string, len(self.UpdatedColumns))
	for i, upCol := range self.UpdatedColumns {
		updated[i] = fmt.Sprintf("%s = %s", q.QuoteColumn(upCol), self.UpdatedValues[i])
	}

	keys := make([]string, len(self.KeyColumns))
	for i, upCol := range self.KeyColumns {
		val := self.KeyValues[i].GetValueSql(q)
		op := "="
		if strings.EqualFold(val, "null") {
			op = "IS"
		}
		keys[i] = fmt.Sprintf("%s %s %s", q.QuoteColumn(upCol), op, val)
	}

	sql := fmt.Sprintf(
		"UPDATE %s\nSET %s",
		self.Table.Qualified(q),
		strings.Join(updated, ",\n    "),
	)

	if len(keys) > 0 {
		sql += "\n" + strings.Join(keys, "  AND ")
	}

	return sql + ";"
}

type DataDelete struct {
	Table      TableRef
	KeyColumns []string
	KeyValues  []ToSqlValue
}

func (self *DataDelete) ToSql(q output.Quoter) string {
	util.Assert(len(self.KeyColumns) == len(self.KeyValues), "len(cols) != len(vals)")
	util.Assert(len(self.KeyColumns) > 0, "DANGER: no key conditions set on DELETE statement")

	cols := make([]string, len(self.KeyColumns))
	for i, col := range self.KeyColumns {
		val := self.KeyValues[i].GetValueSql(q)
		op := "="
		if strings.EqualFold(val, "null") {
			op = "IS"
		}
		cols[i] = fmt.Sprintf("%s %s %s", q.QuoteColumn(col), op, self.KeyValues[i])
	}
	return fmt.Sprintf("DELETE FROM %s\nWHERE %s;", self.Table.Qualified(q), strings.Join(cols, "  AND "))
}
