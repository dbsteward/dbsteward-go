package sql

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/output"
)

// TODO(go,mysql) this was copied verbatim from pgsql; make sure it's correct for mysql

type Qualifiable interface {
	Qualified(q output.Quoter) string
}

type Quotable interface {
	Quoted(q output.Quoter) string
}

type DoNotQuote struct {
	Text string
}

func (self *DoNotQuote) Quoted(q output.Quoter) string {
	return self.Text
}

type QuoteObject struct {
	Ident string
}

func (self *QuoteObject) Quoted(q output.Quoter) string {
	return q.QuoteObject(self.Ident)
}

type SchemaRef struct {
	Schema string
}

func (self *SchemaRef) Qualified(q output.Quoter) string {
	return q.QuoteSchema(self.Schema)
}

func (self *SchemaRef) Quoted(q output.Quoter) string {
	return q.QuoteSchema(self.Schema)
}

type TableRef struct {
	Schema string
	Table  string
}

func (self *TableRef) Qualified(q output.Quoter) string {
	return q.QualifyTable(self.Schema, self.Table)
}

func (self *TableRef) Quoted(q output.Quoter) string {
	return q.QuoteTable(self.Table)
}

func (self *TableRef) QualifiedLiteralString(q output.Quoter) string {
	return q.LiteralString(fmt.Sprintf("%s.%s", self.Schema, self.Table))
}

type ColumnRef struct {
	Schema string
	Table  string
	Column string
}

func (self *ColumnRef) Qualified(q output.Quoter) string {
	return q.QualifyColumn(self.Schema, self.Table, self.Column)
}

func (self *ColumnRef) Quoted(q output.Quoter) string {
	return q.QuoteColumn(self.Column)
}

func (self *ColumnRef) TableRef() *TableRef {
	return &TableRef{self.Schema, self.Table}
}

type SequenceRef struct {
	Schema   string
	Sequence string
}

func (self *SequenceRef) Qualified(q output.Quoter) string {
	return q.QualifyObject(self.Schema, self.Sequence)
}

func (self *SequenceRef) Quoted(q output.Quoter) string {
	return q.QuoteObject(self.Sequence)
}

type TypeRef struct {
	Schema string
	Type   string
}

func ParseTypeRef(spec string) TypeRef {
	parts := strings.Split(spec, ".")
	if len(parts) == 1 {
		return BuiltinTypeRef(parts[0])
	}
	return TypeRef{parts[0], parts[1]}
}

func BuiltinTypeRef(spec string) TypeRef {
	return TypeRef{"", spec}
}

func (self *TypeRef) Qualified(q output.Quoter) string {
	if self.Schema == "" {
		// in the case of builtin names like `text`, there is no schema and we should not quote it
		return self.Type
	}
	return q.QualifyObject(self.Schema, self.Type)
}

func (self *TypeRef) Quoted(q output.Quoter) string {
	if self.Schema == "" {
		// in the case of builtin names like `text`, there is no schema and we should not quote it
		return self.Type
	}
	return q.QuoteObject(self.Type)
}

type TriggerRef struct {
	Schema  string
	Trigger string
}

func (self *TriggerRef) Qualified(q output.Quoter) string {
	return q.QualifyObject(self.Schema, self.Trigger)
}

func (self *TriggerRef) Quoted(q output.Quoter) string {
	return q.QuoteObject(self.Trigger)
}

type ViewRef struct {
	Schema string
	View   string
}

func (self *ViewRef) Qualified(q output.Quoter) string {
	return q.QualifyObject(self.Schema, self.View)
}

func (self *ViewRef) Quoted(q output.Quoter) string {
	return q.QuoteObject(self.View)
}

type IndexRef struct {
	Schema string
	Index  string
}

func (self *IndexRef) Qualified(q output.Quoter) string {
	return q.QualifyObject(self.Schema, self.Index)
}

func (self *IndexRef) Quoted(q output.Quoter) string {
	return q.QuoteObject(self.Index)
}

type FunctionRef struct {
	Schema   string
	Function string
	Params   []string
}

func (self *FunctionRef) Qualified(q output.Quoter) string {
	return fmt.Sprintf("%s(%s)", q.QualifyObject(self.Schema, self.Function), strings.Join(self.Params, ", "))
}

func (self *FunctionRef) Quoted(q output.Quoter) string {
	return q.QuoteObject(self.Function)
}
