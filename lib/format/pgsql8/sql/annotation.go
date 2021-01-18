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

type Comment struct {
	Comment string
}

func NewComment(format string, args ...interface{}) *Comment {
	return &Comment{
		Comment: fmt.Sprintf(format, args...),
	}
}

func (self *Comment) ToSql(q output.Quoter) string {
	return util.PrefixLines(self.Comment, "-- ")
}
