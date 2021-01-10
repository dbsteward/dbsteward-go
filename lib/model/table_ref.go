package model

import "fmt"

type TableRef struct {
	Schema *Schema
	Table  *Table
}

func (self TableRef) String() string {
	return fmt.Sprintf("%s.%s", self.Schema.Name, self.Table.Name)
}
