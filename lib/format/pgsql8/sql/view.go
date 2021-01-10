package sql

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/output"
)

type ViewCreate struct {
	View  ViewRef
	Query string
}

func (self *ViewCreate) ToSql(q output.Quoter) string {
	// TODO(feat) OR REPLACE?
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s\n AS %s;", self.View.Qualified(q), self.Query)
}

type ViewSetComment struct {
	View    ViewRef
	Comment string
}

func (self *ViewSetComment) ToSql(q output.Quoter) string {
	return fmt.Sprintf(
		"COMMENT ON VIEW %s IS %s;",
		self.View.Qualified(q),
		q.LiteralString(self.Comment),
	)
}

type ViewAlterOwner struct {
	View ViewRef
	Role string
}

func (self *ViewAlterOwner) ToSql(q output.Quoter) string {
	return fmt.Sprintf(
		"ALTER VIEW %s OWNER TO %s;",
		self.View.Qualified(q),
		q.QuoteRole(self.Role),
	)
}
