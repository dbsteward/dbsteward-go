package sql

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/output"
)

type TableRef struct {
	Schema string
	Table  string
}

func (self *TableRef) Qualified(q output.Quoter) string {
	return q.QualifyTable(self.Schema, self.Table)
}

func (self *TableRef) Quoted(q output.Quoter) string {
	return q.QuoteTable(self.Table)
}

type TableCreate struct {
	Table        TableRef
	Columns      []TableCreateColumn
	Inherits     *TableRef
	OtherOptions []TableCreateOption // TODO make individual options first-class
}

type TableCreateColumn struct {
	Column string
	Type   string
}

type TableCreateOption struct {
	Option string
	Value  string
}

func (self *TableCreate) ToSql(q output.Quoter) string {
	cols := []string{}
	for _, col := range self.Columns {
		cols = append(cols, fmt.Sprintf("%s %s", q.QuoteColumn(col.Column), col.Type))
	}
	colsql := ""
	if len(cols) > 0 {
		colsql = fmt.Sprintf("\n\t%s\n", strings.Join(cols, ",\n\t"))
	}

	opts := []string{}
	for _, opt := range self.OtherOptions {
		opts = append(opts, fmt.Sprintf("%s %s", strings.ToUpper(opt.Option), opt.Value))
	}
	if self.Inherits != nil {
		opts = append(opts, fmt.Sprintf("INHERITS (%s)", self.Inherits.Qualified(q)))
	}
	optsql := ""
	if len(opts) > 0 {
		optsql = fmt.Sprintf("\n%s", strings.Join(opts, "\n"))
	}

	return fmt.Sprintf(
		"CREATE TABLE %s(%s)%s;",
		self.Table.Qualified(q),
		colsql,
		optsql,
	)
}

type TableSetComment struct {
	Table   TableRef
	Comment string
}

func (self *TableSetComment) ToSql(q output.Quoter) string {
	return fmt.Sprintf(
		"COMMENT ON TABLE %s IS %s;",
		self.Table.Qualified(q),
		q.LiteralString(self.Comment),
	)
}

type TableAlterOwner struct {
	Table TableRef
	Role  string
}

func (self *TableAlterOwner) ToSql(q output.Quoter) string {
	return fmt.Sprintf(
		"ALTER TABLE %s OWNER TO %s;",
		self.Table.Qualified(q),
		q.QuoteRole(self.Role),
	)
}
