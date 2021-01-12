package sql99

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format"
)

type Operations struct {
	format.Operations
}

// NOTE: Sql99.OperationsIface will need to be provided after invoking:
//     parent := &sql99.Sql99{}
//     child := &pgsql8.Pgsql8{parent}
//     child.sql99.OperationsIface = child
// Yes, this is super weird, and a holdover from PHP. TODO(go,3) get rid of this
func NewOperations() *Operations {
	return &Operations{}
}

func (self *Operations) GetQuoteChar() string {
	return `"`
}

func (self *Operations) IsIllegalIdentifier(name string) bool {
	// TODO(go,core) see operations::is_illegal_identifier
	return false
}

func (self *Operations) IsReservedIdentifier(name string) bool {
	// TODO(go,core) see operations::is_identifier_blacklisted
	return false
}

func (self *Operations) GetQuotedName(name string, shouldQuote bool) string {
	dbsteward := lib.GlobalDBSteward
	shouldQuote = shouldQuote || dbsteward.QuoteAllNames
	if !shouldQuote {
		if self.IsIllegalIdentifier(name) {
			if dbsteward.QuoteIllegalIdentifiers {
				dbsteward.Warning("Quoting illegal identifier '%s'", name)
				shouldQuote = true
			} else {
				dbsteward.Fatal("Illegal identifier '%s' - turn on quoting of illegal identifiers with --quoteillegalnames", name)
			}
		} else if self.IsReservedIdentifier(name) {
			if dbsteward.QuoteReservedIdentifiers {
				dbsteward.Warning("Quoting reserved identifier '%s'", name)
				shouldQuote = true
			} else {
				dbsteward.Fatal("Reserved identifier '%s' - turn on quoting of reserved identifiers with --quotereservednames", name)
			}
		}
	}

	if shouldQuote {
		// TODO(feat) do we need to add escaping here?
		quoteChar := self.GetQuoteChar()
		return quoteChar + name + quoteChar
	}
	return name
}

func (self *Operations) QuoteSchema(name string) string {
	return self.GetQuotedName(name, lib.GlobalDBSteward.QuoteSchemaNames)
}

func (self *Operations) QuoteTable(name string) string {
	return self.GetQuotedName(name, lib.GlobalDBSteward.QuoteTableNames)
}

func (self *Operations) QuoteColumn(name string) string {
	return self.GetQuotedName(name, lib.GlobalDBSteward.QuoteColumnNames)
}

func (self *Operations) QuoteRole(name string) string {
	return self.GetQuotedName(name, lib.GlobalDBSteward.QuoteObjectNames)
}

func (self *Operations) QuoteObject(name string) string {
	return self.GetQuotedName(name, lib.GlobalDBSteward.QuoteObjectNames)
}

func (self *Operations) QualifyTable(schema string, table string) string {
	return fmt.Sprintf("%s.%s", self.QuoteSchema(schema), self.QuoteTable(table))
}

func (self *Operations) QualifyObject(schema string, object string) string {
	return fmt.Sprintf("%s.%s", self.QuoteSchema(schema), self.QuoteObject(object))
}

func (self *Operations) QualifyColumn(schema string, table string, column string) string {
	return fmt.Sprintf("%s.%s.%s", self.QuoteSchema(schema), self.QuoteTable(table), self.QuoteColumn(column))
}
