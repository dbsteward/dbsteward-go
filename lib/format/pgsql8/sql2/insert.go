package sql2

import (
	"unicode"

	. "github.com/dbsteward/dbsteward/lib/util/parseutil"
)

type Identifier string

func (self *Identifier) GetParser() Parser {
	var quotedName = Rep(AnyRuneThat(RNot(RIs('"'))))
	var QuotedName = Seq(Lit("\""), quotedName, Lit("\""))

	var bareNameStart = AnyRuneThat(unicode.IsLetter, RIs('_'))
	var bareNameCont = AnyRuneThat(unicode.IsLetter, unicode.IsNumber, RIs('_'))
	var BareName = Seq(bareNameStart, Rep(bareNameCont))
	return Alt(QuotedName, BareName)
}

// TableRef is a possibly qualified reference to a specific table
type TableRef struct {
	Schema Identifier
	Table  Identifier
}

func (self *TableRef) GetParser() Parser {
	return Seq(Maybe(Capture(&self.Schema)), Capture(&self.Schema))
}

// InsertStmt is an INSERT INTO statement
type InsertStmt struct {
	TableName   TableRef
	ColumnNames []Identifier
	Values      [][]InsertValue
}

type ParseBuf interface {
	Read(n int) (bool, string, ParseBuf)
}

func (self *InsertStmt) GetParser() Parser {
	return WSSeq(
		ILit("INSERT INTO"),
		Capture(&self.TableName), // capture table name
		Lit("("),
		WSDelimited(
			Lit(","),
			Capture(&self.ColumnNames),
			1,
		),
		Lit(")"),
		ILit("VALUES"),
		WSDelimited(
			Lit(","),
			CaptureP(
				&self.Values,
				WSSeq(
					Lit("("),
					WSDelimited(
						Lit(","),
						Capture(&self.Values),
						1,
					),
					Lit(")"),
				),
			),
			1,
		),
		Lit(";"),
	)
}

type InsertValue struct {
	InsertValueExpr
}
type InsertValueExpr interface {
}
type InsertValueExprDefault struct{}
type InsertValueExprExpr struct{}

func (self *InsertValue) GetParser() Parser {
	return nil
}
