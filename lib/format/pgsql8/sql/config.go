package sql

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/output"
)

type ConfigParamSet struct {
	Name  string
	Value string
}

func (self *ConfigParamSet) ToSql(q output.Quoter) string {
	return fmt.Sprintf("SELECT dbsteward.db_config_parameter(%s, %s)", q.LiteralString(self.Name), q.LiteralString(self.Value))
}
