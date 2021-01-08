package sql

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/output"
)

type Annotated struct {
	Wrapped    output.ToSql
	Annotation string
}

func (self *Annotated) ToSql(q output.Quoter) string {
	return fmt.Sprintf(
		"-- %s\n%s",
		strings.ReplaceAll(self.Annotation, "\n", "\n-- "),
		self.Wrapped.ToSql(q),
	)
}
