package sql

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

type IndexCreate struct {
	Table        TableRef
	Index        string
	Unique       bool
	Concurrently bool
	Using        string
	Dimensions   []Quotable
	Where        string
}

func (self *IndexCreate) ToSql(q output.Quoter) string {
	parts := []string{
		"CREATE",
		util.MaybeStr(self.Unique, "UNIQUE"),
		"INDEX",
		util.MaybeStr(self.Concurrently, "CONCURRENTLY"),
		q.QuoteObject(self.Index),
		"ON",
		self.Table.Qualified(q),
		util.MaybeStr(self.Using != "", "USING "+self.Using),
	}

	dims := make([]string, len(self.Dimensions))
	for i, dim := range self.Dimensions {
		dims[i] = dim.Quoted(q)
	}
	parts = append(parts,
		fmt.Sprintf("(%s)", strings.Join(dims, ", ")),
		util.MaybeStr(self.Where != "", fmt.Sprintf("WHERE (%s)", self.Where)),
	)
	return util.CondJoin(" ", parts...)
}
