package ir

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
)

type Key struct {
	Schema  *Schema
	Table   *Table
	Columns []*Column
	KeyName string
}

func (self *Key) String() string {
	cols := make([]string, len(self.Columns))
	for i, col := range self.Columns {
		cols[i] = col.Name
	}
	return fmt.Sprintf(
		"%s %s (%s)",
		util.CoalesceStr(self.KeyName, "unnamed key"),
		util.CondJoin(".", self.Schema.Name, self.Table.Name),
		util.CoalesceStr(strings.Join(cols, ","), "*"),
	)
}

type KeyNames struct {
	Schema  string
	Table   string
	Columns []string
	KeyName string
}

func (self *KeyNames) String() string {
	return fmt.Sprintf(
		"%s %s (%s)",
		util.CoalesceStr(self.KeyName, "unnamed key"),
		util.CondJoin(".", self.Schema, self.Table),
		util.CoalesceStr(strings.Join(self.Columns, ","), "*"),
	)
}
