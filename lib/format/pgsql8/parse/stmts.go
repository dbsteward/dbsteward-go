package parse

import (
	"unicode"

	. "github.com/dbsteward/dbsteward/lib/util/parseutil"
)

var Statement = Alt(
	InsertStmt,
	DeleteStmt,
)

var InsertStmt = WSSeq(
	ILit("INSERT INTO"),
	QualifiedName, // capture table name
	Lit("("),
	WSDelimited(
		Lit(","),
		QualifiedName, // capture col name
		1,
	),
	Lit(")"),
	ILit("VALUES"),
	WSDelimited(
		Lit(","),
		WSSeq(
			Lit("("),
			WSDelimited(
				Lit(","),
				SqlInsertExpr, // capture col value
				1,
			),
			Lit(")"),
		), // capture row
		1,
	),
	Lit(";"),
) // capture insert

var SqlInsertExpr = Alt(
	SqlExpr,
	ILit("DEFAULT"),
)

var DeleteStmt = Seq()

var SqlExpr = WSAlt(
	SqlLit,
	// TODO
)
var SqlLit = WSAlt(
	SqlString,
	SqlNumber,
	SqlBoolean,
	SqlNull,
)
var SqlString = Seq( /*TODO*/ )
var SqlNumber = Seq( /*TODO*/ )
var SqlNull = ILit("NULL")
var SqlBoolean = Alt(
	ILit("t"), ILit("true"),
	ILit("f"), ILit("false"),
)
var QualifiedName = WSDelimited(Lit("."), Name, 1, 2)
var Name = Alt(
	QuotedName,
	BareName,
)
var QuotedName = Seq(Lit("\""), quotedName, Lit("\""))
var quotedName = Rep(AnyRuneThat(RNot(RIs('"'))))
var BareName = Seq(bareNameStart, Rep(bareNameCont))
var bareNameStart = AnyRuneThat(unicode.IsLetter, RIs('_'))
var bareNameCont = AnyRuneThat(unicode.IsLetter, unicode.IsNumber, RIs('_'))
