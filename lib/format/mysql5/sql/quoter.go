package sql

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
)

type Quoter struct {
	Logger util.Logger

	ShouldQuoteSchemaNames         bool
	ShouldQuoteTableNames          bool
	ShouldQuoteColumnNames         bool
	ShouldQuoteObjectNames         bool
	ShouldQuoteIllegalIdentifiers  bool
	ShouldQuoteReservedIdentifiers bool
}

func (self *Quoter) isIllegalIdentifier(name string) bool {
	// TODO(go,core) see operations::is_illegal_identifier
	// will need to import a list, probably bake it into the binary or just suck it up and make a giant constant list
	return false
}

func (self *Quoter) isReservedIdentifier(name string) bool {
	// TODO(go,core) see operations::is_identifier_blacklisted
	// will need to import a list, probably bake it into the binary or just suck it up and make a giant constant list
	return false
}

func (self *Quoter) getQuotedName(name string, shouldQuote bool) string {
	if !shouldQuote {
		if self.isIllegalIdentifier(name) {
			if self.ShouldQuoteIllegalIdentifiers {
				self.Logger.Warning("Quoting illegal identifier '%s'", name)
				shouldQuote = true
			} else {
				self.Logger.Fatal("Illegal identifier '%s' - turn on quoting of illegal identifiers with --quoteillegalnames", name)
			}
		} else if self.isReservedIdentifier(name) {
			if self.ShouldQuoteReservedIdentifiers {
				self.Logger.Warning("Quoting reserved identifier '%s'", name)
				shouldQuote = true
			} else {
				self.Logger.Fatal("Reserved identifier '%s' - turn on quoting of reserved identifiers with --quotereservednames", name)
			}
		}
	}

	if shouldQuote {
		return fmt.Sprintf(`"%s"`, strings.ReplaceAll(name, `"`, `""`))
	}
	return name
}

func (self *Quoter) QuoteSchema(name string) string {
	return self.getQuotedName(name, self.ShouldQuoteSchemaNames)
}

func (self *Quoter) QuoteTable(name string) string {
	return self.getQuotedName(name, self.ShouldQuoteTableNames)
}

func (self *Quoter) QuoteColumn(name string) string {
	return self.getQuotedName(name, self.ShouldQuoteColumnNames)
}

func (self *Quoter) QuoteRole(name string) string {
	return self.getQuotedName(name, self.ShouldQuoteObjectNames)
}

func (self *Quoter) QuoteObject(name string) string {
	return self.getQuotedName(name, self.ShouldQuoteObjectNames)
}

func (self *Quoter) QualifyTable(schema string, table string) string {
	return fmt.Sprintf("%s.%s", self.QuoteSchema(schema), self.QuoteTable(table))
}

func (self *Quoter) QualifyObject(schema string, object string) string {
	return fmt.Sprintf("%s.%s", self.QuoteSchema(schema), self.QuoteObject(object))
}

func (self *Quoter) QualifyColumn(schema string, table string, column string) string {
	return fmt.Sprintf("%s.%s.%s", self.QuoteSchema(schema), self.QuoteTable(table), self.QuoteColumn(column))
}

func (self *Quoter) LiteralString(value string) string {
	return fmt.Sprintf("'%s'", strings.ReplaceAll(value, "'", "''"))
}

func (self *Quoter) LiteralValue(datatype, value string, isNull bool) string {
	// TODO(go,3) it'd be amazing to have a dedicated Value type that encapsulates this logic and is type-aware,
	// instead of the mishmash of string parsing and type matching we do

	if isNull {
		return "NULL"
	}

	// datatypes that should be encoded as strings
	if util.IMatch(`^(bool.*|character.*|string|text|date|time.*|(var)?char.*)`, datatype) != nil {
		return self.LiteralString(value)
	}

	return value
}
