package sql

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

type Annotated struct {
	Wrapped    output.ToSql
	Annotation string
}

func (self *Annotated) ToSql(q output.Quoter) string {
	return fmt.Sprintf(
		"%s\n%s",
		util.PrefixLines(self.Annotation, "-- "),
		self.Wrapped.ToSql(q),
	)
}
