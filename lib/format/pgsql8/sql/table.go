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

type CreateTable struct {
	Table        TableRef
	Columns      []CreateTableColumn
	Inherits     *TableRef
	OtherOptions []CreateTableOption // TODO make individual options first-class
}

type CreateTableColumn struct {
	Column string
	Type   string
}

type CreateTableOption struct {
	Option string
	Value  string
}

func (self *CreateTable) ToSql(q output.Quoter) string {
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
		"CREATE TABLE %s(%s)%s;\n",
		self.Table.Qualified(q),
		colsql,
		optsql,
	)
}

type SetTableComment struct {
	Table   TableRef
	Comment string
}

func (self *SetTableComment) ToSql(q output.Quoter) string {
	return fmt.Sprintf(
		"COMMENT ON TABLE %s IS %s;\n",
		self.Table.Qualified(q),
		q.LiteralString(self.Comment),
	)
}

type AlterTableOwner struct {
	Table TableRef
	Role  string
}

func (self *AlterTableOwner) ToSql(q output.Quoter) string {
	return fmt.Sprintf(
		"ALTER TABLE %s OWNER TO %s;\n",
		self.Table.Qualified(q),
		q.QuoteRole(self.Role),
	)
}
