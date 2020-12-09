package sql

import (
	"fmt"
)

type SetValSerialSequenceMax struct {
	Schema string
	Table  string
	Column string
}

func (self *SetValSerialSequenceMax) ToSql() string {
	// TODO(go,core) quoting
	return fmt.Sprintf(
		`SELECT setval(pg_get_serial_sequence('%s.%s', '%s'), MAX(%[3]s), true) FROM %[1]s.%[2]s;`,
		self.Schema, self.Table, self.Column,
	)
}
