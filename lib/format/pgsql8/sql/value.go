package sql

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/output"
)

type ToSqlValue interface {
	GetValueSql(q output.Quoter) string
}

var ValueDefault = RawSql("DEFAULT")
var ValueNull = RawSql("NULL")

type RawSql string

func (self RawSql) GetValueSql(q output.Quoter) string {
	return string(self)
}

func (self RawSql) ToSql(q output.Quoter) string {
	return string(self)
}

type IntValue int

func (self IntValue) GetValueSql(q output.Quoter) string {
	return fmt.Sprintf("%d", int(self))
}

type BoolValue bool

func (self BoolValue) GetValueSql(q output.Quoter) string {
	return fmt.Sprintf("%t", bool(self))
}

type StringValue string

func (self StringValue) GetValueSql(q output.Quoter) string {
	return q.LiteralString(string(self))
}

// EscapedStringValues get turned into E'...'
type EscapedStringValue string

func (self EscapedStringValue) GetValueSql(q output.Quoter) string {
	return q.LiteralStringEscaped(string(self))
}

// ExpressionValues are self-contained SQL expressions wrapped in parentheses
type ExpressionValue string

func (self ExpressionValue) GetValueSql(q output.Quoter) string {
	return fmt.Sprintf("(%s)", string(self))
}

// TypedValues are string-encoded literal values of a dynamic SQL type that might need to be formatted/escaped
type TypedValue struct {
	Type  string
	Value string
}

func (self *TypedValue) GetValueSql(q output.Quoter) string {
	return q.LiteralValue(self.Type, self.Value)
}
