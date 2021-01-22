package sql

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/output"
)

type SchemaCreate struct {
	Schema string
}

func (self *SchemaCreate) ToSql(q output.Quoter) string {
	return fmt.Sprintf("CREATE SCHEMA %s;", q.QuoteSchema(self.Schema))
}

type SchemaDrop struct {
	Schema  string
	Cascade bool
}

func (self *SchemaDrop) ToSql(q output.Quoter) string {
	sql := fmt.Sprintf("DROP SCHEMA %s", q.QuoteSchema(self.Schema))
	if self.Cascade {
		sql += " CASCADE"
	}
	return sql + ";"
}

type SchemaAlterOwner struct {
	Schema string
	Owner  string
}

func (self *SchemaAlterOwner) ToSql(q output.Quoter) string {
	return fmt.Sprintf("ALTER SCHEMA %s OWNER TO %s;", q.QuoteSchema(self.Schema), q.QuoteRole(self.Owner))
}

type SchemaSetComment struct {
	Schema  string
	Comment string
}

func (self *SchemaSetComment) ToSql(q output.Quoter) string {
	return fmt.Sprintf("COMMENT ON SCHEMA %s IS %s;", q.QuoteSchema(self.Schema), q.LiteralStringEscaped(self.Comment))
}

type SchemaGrant struct {
	Schema   string
	Perms    []string
	Roles    []string
	CanGrant bool
}

func (self *SchemaGrant) ToSql(q output.Quoter) string {
	return (&grant{
		grantTypeSchema,
		&SchemaRef{self.Schema},
		self.Perms,
		self.Roles,
		self.CanGrant,
	}).ToSql(q)
}
