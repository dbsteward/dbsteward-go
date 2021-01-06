package sql

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/output"
)

type grantType string

const (
	grantTypeSchema   grantType = "SCHEMA"
	grantTypeTable    grantType = "TABLE"
	grantTypeSequence grantType = "SEQUENCE"
	grantTypeFunction grantType = "FUNCTION"
)

// to avoid a LOT of code duplication among the various *Grant types
type grant struct {
	ObjType  grantType
	Object   Qualifiable
	Perms    []string
	Roles    []string
	CanGrant bool
}

func (self *grant) ToSql(q output.Quoter) string {
	roles := make([]string, len(self.Roles))
	for i, role := range self.Roles {
		// the PUBLIC role is actually a keyword, not an identifier, so don't quote it
		if strings.EqualFold(role, "public") {
			roles[i] = role
		} else {
			roles[i] = q.QuoteRole(role)
		}
	}

	// NOTE it is the job of callers to validate that the correct permissions are set
	perms := make([]string, len(self.Perms))
	for i, perm := range self.Perms {
		perms[i] = strings.ToUpper(perm)
	}

	option := ""
	if self.CanGrant {
		option = " WITH GRANT OPTION"
	}

	return fmt.Sprintf(
		"GRANT %s ON %s %s TO %s%s;",
		strings.Join(perms, ", "),
		self.ObjType,
		self.Object.Qualified(q),
		strings.Join(roles, ", "),
		option,
	)
}
