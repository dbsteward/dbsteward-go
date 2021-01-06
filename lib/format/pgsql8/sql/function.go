package sql

import "github.com/dbsteward/dbsteward/lib/output"

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
