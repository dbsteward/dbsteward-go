package sql99

import (
	"github.com/dbsteward/dbsteward/lib"
)

// NOTE: If you add new functions here, but do not add them to Sql99 or concrete structs,
// there will not be a compiler error, because Sql99 embeds Operations and concretes embed Sql99
// but there _will_ be a runtime error, because the method pointer will be nil
type Operations interface {
	GetQuoteChar() string
	IsIllegalIdentifier(string) bool
	IsReservedIdentifier(string) bool
	GetQuotedName(name string, shouldQuote bool) string
	GetQuotedColumnName(name string) string
}

type Sql99 struct {
	Operations
}

// NOTE: Sql99.AbstractOperations will need to be provided after invoking:
// 	parent := &sql99.Sql99{}
//  child := &pgsql8.Pgsql8{parent}
//  child.sql99.Operations = child
// Yes, this is super weird, and a holdover from PHP. TODO(go,3) get rid of this
func NewSql99() *Sql99 {
	return &Sql99{}
}

func (self *Sql99) GetQuoteChar() string {
	return `"`
}

func (self *Sql99) IsIllegalIdentifier(name string) bool {
	// TODO(go,core) see sql99::is_illegal_identifier
	return false
}

func (self *Sql99) IsReservedIdentifier(name string) bool {
	// TODO(go,core) see sql99::is_identifier_blacklisted
	return false
}

func (self *Sql99) GetQuotedName(name string, shouldQuote bool) string {
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

func (self *Sql99) GetQuotedColumnName(name string) string {
	return self.GetQuotedName(name, lib.GlobalDBSteward.QuoteColumnNames)
}
