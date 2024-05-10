package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/ir"
)

var commonSchema = NewSchema()

func init() {
	lib.RegisterFormat(ir.SqlFormatPgsql8, NewOperations)
}
