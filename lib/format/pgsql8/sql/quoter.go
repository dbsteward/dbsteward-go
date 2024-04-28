package sql

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/dbsteward/dbsteward/lib/util"
)

// TODO(go,nth) consider replacing this whole thing with e.g. ToIdentifier interface similar to ToSqlValue
// Operations can have a factory method that consults lib.GlobalDBSteward configuration for quotedness
// Would also give us a nice platform for the later quoting/casing changes (see README)

type Quoter struct {
	Logger util.Logger

	ShouldQuoteSchemaNames         bool
	ShouldQuoteTableNames          bool
	ShouldQuoteColumnNames         bool
	ShouldQuoteObjectNames         bool
	ShouldQuoteIllegalIdentifiers  bool
	ShouldQuoteReservedIdentifiers bool
	ShouldEEscape                  bool
	RequireVerboseIntervalNotation bool
}

func (quoter *Quoter) isIllegalIdentifier(_ string) bool {
	// TODO(go,core) see operations::is_illegal_identifier
	// will need to import a list, probably bake it into the binary or just suck it up and make a giant constant list
	return false
}

func isReservedIdentifier(_ string) bool {
	// TODO(go,core) see operations::is_identifier_blacklisted
	// will need to import a list, probably bake it into the binary or just suck it up and make a giant constant list
	return false
}

// identifierNeedsQuoted returns true if the identifier would
// be invalid if not quoted. This is a bit fuzzy because sometimes
// the quoting requirement depends on the command context.
// This function leans toward safety by returning true any
// time an identifier *might* require quoting.
func identifierNeedsQuoted(i string) bool {
	if strings.ContainsAny(i, "\" ,.$") {
		return true
	}
	// first character is limited to letters and _
	runes := []rune(i)
	if !unicode.IsLetter(runes[0]) && runes[0] != '_' {
		return true
	}
	if isReservedIdentifier(i) {
		return true
	}
	return false
}

func (quoter *Quoter) getQuotedName(name string, shouldQuote bool) string {
	if !shouldQuote {
		if quoter.isIllegalIdentifier(name) {
			if quoter.ShouldQuoteIllegalIdentifiers {
				quoter.Logger.Warning("Quoting illegal identifier '%s'", name)
				shouldQuote = true
			} else {
				quoter.Logger.Fatal("Illegal identifier '%s' - turn on quoting of illegal identifiers with --quoteillegalnames", name)
			}
		} else if identifierNeedsQuoted(name) {
			quoter.Logger.Warning("Quoting identifier '%s'", name)
			shouldQuote = true
		}
	}
	if shouldQuote {
		return fmt.Sprintf(`"%s"`, strings.ReplaceAll(name, `"`, `""`))
	}
	return name
}

func (quoter *Quoter) QuoteSchema(name string) string {
	return quoter.getQuotedName(name, quoter.ShouldQuoteSchemaNames)
}

func (quoter *Quoter) QuoteTable(name string) string {
	return quoter.getQuotedName(name, quoter.ShouldQuoteTableNames)
}

func (quoter *Quoter) QuoteColumn(name string) string {
	return quoter.getQuotedName(name, quoter.ShouldQuoteColumnNames)
}

func (quoter *Quoter) QuoteRole(name string) string {
	return quoter.getQuotedName(name, quoter.ShouldQuoteObjectNames)
}

func (quoter *Quoter) QuoteObject(name string) string {
	return quoter.getQuotedName(name, quoter.ShouldQuoteObjectNames)
}

func (quoter *Quoter) QualifyTable(schema string, table string) string {
	return fmt.Sprintf("%s.%s", quoter.QuoteSchema(schema), quoter.QuoteTable(table))
}

func (quoter *Quoter) QualifyObject(schema string, object string) string {
	return fmt.Sprintf("%s.%s", quoter.QuoteSchema(schema), quoter.QuoteObject(object))
}

func (quoter *Quoter) QualifyColumn(schema string, table string, column string) string {
	return fmt.Sprintf("%s.%s.%s", quoter.QuoteSchema(schema), quoter.QuoteTable(table), quoter.QuoteColumn(column))
}

func (quoter *Quoter) LiteralString(value string) string {
	out := fmt.Sprintf("'%s'", strings.ReplaceAll(value, "'", "''"))
	if quoter.ShouldEEscape {
		// TODO(go,pgsql) this is definitely not all there is to it... is it?
		// will need to study https://www.postgresql.org/docs/13/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS-ESCAPE
		return "E" + out
	}
	return out
}

func (quoter *Quoter) LiteralValue(datatype, value string, isNull bool) string {
	// TODO(go,3) it'd be amazing to have a dedicated Value type that encapsulates this logic and is type-aware,
	// instead of the mishmash of string parsing and type matching we do

	if isNull {
		return "NULL"
	}

	// complain when we require verbose interval notation but data uses a different format
	if quoter.RequireVerboseIntervalNotation && util.IMatch("interval", datatype) != nil && value[0] != '@' {
		quoter.Logger.Fatal("bad interval value: '%s' -- interval types must be postgresql verbose format: '@ 2 hours 30 minutes'", value)
	}

	// datatypes that should be encoded as strings
	if util.IMatch(`^(bool.*|character.*|string|text|date|time.*|(var)?char.*|interval|money|inet|uuid|ltree)`, datatype) != nil {
		return quoter.LiteralString(value)
	}

	return value
}
