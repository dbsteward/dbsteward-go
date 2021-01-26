package sql

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/output"
)

// TODO(go,3) at what point should we just pass the whole model.Trigger object?

type TriggerCreate struct {
	Trigger  TriggerRef
	Table    TableRef
	Events   []string
	Timing   string
	ForEach  string
	Function string
}

func (self *TriggerCreate) ToSql(q output.Quoter) string {
	return fmt.Sprintf(
		"CREATE TRIGGER %s\n  %s %s\n  ON %s\n  FOR EACH %s\n  EXECUTE PROCEDURE %s;",
		self.Trigger.Quoted(q),
		self.Timing,
		strings.Join(self.Events, " OR "),
		self.Table.Qualified(q),
		self.ForEach,
		self.Function, // TODO(feat) should be a full FunctionRef
	)
}

type TriggerDrop struct {
	Trigger TriggerRef
	Table   TableRef
}

func (self *TriggerDrop) ToSql(q output.Quoter) string {
	return fmt.Sprintf("DROP TRIGGER %s ON %s;", self.Trigger.Qualified(q), self.Table.Qualified(q))
}
