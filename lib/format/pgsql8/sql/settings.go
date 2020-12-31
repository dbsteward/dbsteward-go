package sql

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/output"
)

type SetCheckFunctionBodies struct {
	Info string
}

func (self *SetCheckFunctionBodies) ToSql(output.Quoter) string {
	return fmt.Sprintf(`SET check_function_bodies = FALSE; -- DBSteward %s`, self.Info)
}
