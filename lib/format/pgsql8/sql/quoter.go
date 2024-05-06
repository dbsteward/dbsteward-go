package sql

import (
	"fmt"
	"log/slog"
	"strings"
	"unicode"

	"github.com/dbsteward/dbsteward/lib/util"
)

// TODO(go,nth) consider replacing this whole thing with e.g. ToIdentifier interface similar to ToSqlValue
// Operations can have a factory method that consults lib.GlobalDBSteward configuration for quotedness
// Would also give us a nice platform for the later quoting/casing changes (see README)

type Quoter struct {
	Logger *slog.Logger

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

// invalidIdentifiers is the list of words that are invalid to
// define as an identifier. See also the comment on isReservedIdentifier
var invalidIdentifiers = map[string]interface{}{
	"ALL":               nil,
	"ANALYSE":           nil,
	"ANALYZE":           nil,
	"AND":               nil,
	"ANY":               nil,
	"ARRAY":             nil,
	"AS":                nil,
	"ASC":               nil,
	"ASYMMETRIC":        nil,
	"AUTHORIZATION":     nil,
	"BINARY":            nil,
	"BOTH":              nil,
	"CASE":              nil,
	"CAST":              nil,
	"CHECK":             nil,
	"COLLATE":           nil,
	"COLUMN":            nil,
	"CONCURRENTLY":      nil,
	"CONSTRAINT":        nil,
	"CREATE":            nil,
	"CROSS":             nil,
	"CURRENT_CATALOG":   nil,
	"CURRENT_DATE":      nil,
	"CURRENT_ROLE":      nil,
	"CURRENT_TIME":      nil,
	"CURRENT_TIMESTAMP": nil,
	"CURRENT_USER":      nil,
	"DEFAULT":           nil,
	"DEFERRABLE":        nil,
	"DESC":              nil,
	"DISTINCT":          nil,
	"DO":                nil,
	"ELSE":              nil,
	"END":               nil,
	"EXCEPT":            nil,
	"FALSE":             nil,
	"FETCH":             nil,
	"FOR":               nil,
	"FOREIGN":           nil,
	"FREEZE":            nil,
	"FROM":              nil,
	"FULL":              nil,
	"GRANT":             nil,
	"GROUP":             nil,
	"HAVING":            nil,
	"ILIKE":             nil,
	"IN":                nil,
	"INITIALLY":         nil,
	"INNER":             nil,
	"INTERSECT":         nil,
	"INTO":              nil,
	"IS":                nil,
	"ISNULL":            nil,
	"JOIN":              nil,
	"LATERAL":           nil,
	"LEADING":           nil,
	"LEFT":              nil,
	"LIKE":              nil,
	"LIMIT":             nil,
	"LOCALTIME":         nil,
	"LOCALTIMESTAMP":    nil,
	"NATURAL":           nil,
	"NOT":               nil,
	"NOTNULL":           nil,
	"NULL":              nil,
	"OFFSET":            nil,
	"ON":                nil,
	"ONLY":              nil,
	"OR	":               nil,
	"ORDER":             nil,
	"OUTER":             nil,
	"OVERLAPS":          nil,
	"PLACING":           nil,
	"PRIMARY":           nil,
	"REFERENCES":        nil,
	"RETURNING":         nil,
	"RIGHT":             nil,
	"SELECT":            nil,
	"SESSION_USER":      nil,
	"SIMILAR":           nil,
	"SOME":              nil,
	"SYMMETRIC":         nil,
	"SYSTEM_USER":       nil,
	"TABLE":             nil,
	"TABLESAMPLE":       nil,
	"THEN":              nil,
	"TO":                nil,
	"TRAILING":          nil,
	"TRUE":              nil,
	"UNION":             nil,
	"UNIQUE":            nil,
	"USER":              nil,
	"USING":             nil,
	"VARIADIC":          nil,
	"VERBOSE":           nil,
	"WHEN":              nil,
	"WHERE":             nil,
	"WINDOW":            nil,
	"WITH":              nil,
}

// isReservedIdentifier returns true if the identifier is a
// reserved SQL key word that could be invalid in some
// situations. The actual rules on this are surprisingly
// complicated, see:
// https://www.postgresql.org/docs/current/sql-keywords-appendix.html
// As a result, this function is best effort.
func isReservedIdentifier(name string) bool {
	if _, isReserved := invalidIdentifiers[strings.ToUpper(name)]; isReserved {
		return true
	}
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
				quoter.Logger.Warn(fmt.Sprintf("Quoting illegal identifier '%s'", name))
				shouldQuote = true
			} else {
				quoter.Logger.Error(fmt.Sprintf("Illegal identifier '%s' - turn on quoting of illegal identifiers with --quoteillegalnames", name))
			}
		} else if identifierNeedsQuoted(name) {
			quoter.Logger.Warn(fmt.Sprintf("Quoting identifier '%s'", name))
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
		quoter.Logger.Error(fmt.Sprintf("bad interval value: '%s' -- interval types must be postgresql verbose format: '@ 2 hours 30 minutes'", value))
	}

	// datatypes that should be encoded as strings
	if util.IMatch(`^(bool.*|character.*|string|text|date|time.*|(var)?char.*|interval|money|inet|uuid|ltree)`, datatype) != nil {
		return quoter.LiteralString(value)
	}

	return value
}
