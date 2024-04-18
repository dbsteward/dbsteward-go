package sql

import (
	"fmt"
	"log"
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

func (ic *IndexCreate) ToSql(q output.Quoter) string {
	parts := []string{
		"CREATE",
		util.MaybeStr(ic.Unique, "UNIQUE"),
		"INDEX",
		util.MaybeStr(ic.Concurrently, "CONCURRENTLY"),
		q.QuoteObject(ic.Index),
		"ON",
		ic.Table.Qualified(q),
		util.MaybeStr(ic.Using != "", "USING "+ic.Using),
	}
	if len(ic.Dimensions) == 0 {
		log.Panicf("Index %s.%s missing dimensions", ic.Table.Qualified(q), ic.Index)
	}
	dims := make([]string, len(ic.Dimensions))
	for i, dim := range ic.Dimensions {
		dims[i] = dim.Quoted(q)
	}
	parts = append(parts,
		fmt.Sprintf("(%s)", strings.Join(dims, ", ")),
		util.MaybeStr(ic.Where != "", fmt.Sprintf("WHERE (%s)", ic.Where)),
	)
	return util.CondJoin(" ", parts...)
}

type IndexDrop struct {
	Index IndexRef
}

func (self *IndexDrop) ToSql(q output.Quoter) string {
	return fmt.Sprintf("DROP INDEX %s;", self.Index.Qualified(q))
}
