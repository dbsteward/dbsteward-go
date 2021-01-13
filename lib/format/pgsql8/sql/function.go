package sql

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

type FunctionCreate struct {
	Function        FunctionRef
	Returns         string
	Definition      string
	Language        string
	CachePolicy     string
	SecurityDefiner bool
}

func (self *FunctionCreate) ToSql(q output.Quoter) string {
	// TODO(feat) should this have `OR REPLACE`?
	ddl := fmt.Sprintf("CREATE OR REPLACE FUNCTION %s RETURNS %s", self.Function.Qualified(q), self.Returns)
	ddl += fmt.Sprintf("\nAS $_$\n%s\n$_$", util.PrefixLines(self.Definition, "  "))
	ddl += "\nLANGUAGE " + self.Language
	if self.CachePolicy != "" {
		ddl += "\n" + self.CachePolicy
	}
	if self.SecurityDefiner {
		ddl += "\nSECURITY DEFINER"
	}
	return ddl + ";"
}

type FunctionGrant struct {
	Function FunctionRef
	Perms    []string
	Roles    []string
	CanGrant bool
}

func (self *FunctionGrant) ToSql(q output.Quoter) string {
	return (&grant{
		grantTypeFunction,
		&self.Function,
		self.Perms,
		self.Roles,
		self.CanGrant,
	}).ToSql(q)
}

type FunctionAlterOwner struct {
	Function FunctionRef
	Role     string
}

func (self *FunctionAlterOwner) ToSql(q output.Quoter) string {
	return fmt.Sprintf(
		"ALTER FUNCTION %s OWNER TO %s;",
		self.Function.Qualified(q),
		q.QuoteRole(self.Role),
	)
}

type FunctionSetComment struct {
	Function FunctionRef
	Comment  string
}

func (self *FunctionSetComment) ToSql(q output.Quoter) string {
	return fmt.Sprintf(
		"COMMENT ON FUNCTION %s IS %s;",
		self.Function.Qualified(q),
		q.LiteralString(self.Comment),
	)
}
