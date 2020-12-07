package sql

import "fmt"

type SetCheckFunctionBodies struct {
	Info string
}

func (self *SetCheckFunctionBodies) ToSql() string {
	return fmt.Sprintf(`SET check_function_bodies = FALSE; -- DBSteward %s`, self.Info)
}
