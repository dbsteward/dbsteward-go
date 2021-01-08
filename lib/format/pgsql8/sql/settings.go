package sql

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/output"
)

type SetCheckFunctionBodies struct {
	Value bool
}

func (self *SetCheckFunctionBodies) ToSql(q output.Quoter) string {
	return fmt.Sprintf(`SET check_function_bodies = %t;`, self.Value)
}
