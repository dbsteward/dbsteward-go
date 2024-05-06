package sql

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

// TODO(go,nth) it would be nice to have something like an AnnotatedGroup,
// which wraps a []output.ToSql and demarcates the beginning and end of the group

type Annotated struct {
	Wrapped    output.ToSql
	Annotation string
}

func (an *Annotated) ToSql(q output.Quoter) string {
	return fmt.Sprintf(
		"%s\n%s",
		util.PrefixLines(an.Annotation, "-- "),
		an.Wrapped.ToSql(q),
	)
}

func (an *Annotated) StripAnnotation() output.ToSql {
	return an.Wrapped
}

type Comment string

func NewComment(format string, args ...interface{}) Comment {
	return Comment(fmt.Sprintf(format, args...))
}

func (c Comment) Comment() string {
	return util.PrefixLines(string(c), "-- ")
}

func (c Comment) ToSql(_ output.Quoter) string {
	return c.Comment()
}
