package sql

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/output"
)

type ToSqlValue interface {
	GetValueSql(q output.Quoter) string
}

type RawSql struct {
	Sql string
}

func (self *RawSql) GetValueSql(q output.Quoter) string {
	return self.Sql
}

type RawIntValue struct {
	Value int
}

func (self *RawIntValue) GetValueSql(q output.Quoter) string {
	return fmt.Sprintf("%d", self.Value)
}
