package sql

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/output"
)

type Qualifiable interface {
	Qualified(q output.Quoter) string
}

type Quotable interface {
	Quoted(q output.Quoter) string
}

type DoNotQuote struct {
	Text string
}

func (qo *DoNotQuote) Quoted(q output.Quoter) string {
	return qo.Text
}

type QuoteObject struct {
	Ident string
}

func (qo *QuoteObject) Quoted(q output.Quoter) string {
	return q.QuoteObject(qo.Ident)
}

type SchemaRef struct {
	Schema string
}

func (sr *SchemaRef) Qualified(q output.Quoter) string {
	return q.QuoteSchema(sr.Schema)
}

func (sr *SchemaRef) Quoted(q output.Quoter) string {
	return q.QuoteSchema(sr.Schema)
}

type TableRef struct {
	Schema string
	Table  string
}

func (tr *TableRef) Qualified(q output.Quoter) string {
	return q.QualifyTable(tr.Schema, tr.Table)
}

func (tr *TableRef) Quoted(q output.Quoter) string {
	return q.QuoteTable(tr.Table)
}

func (tr *TableRef) QualifiedLiteralString(q output.Quoter) string {
	return q.LiteralString(fmt.Sprintf("%s.%s", tr.Schema, tr.Table))
}

type ColumnRef struct {
	Schema string
	Table  string
	Column string
}

func (cr *ColumnRef) Qualified(q output.Quoter) string {
	return q.QualifyColumn(cr.Schema, cr.Table, cr.Column)
}

func (cr *ColumnRef) Quoted(q output.Quoter) string {
	return q.QuoteColumn(cr.Column)
}

func (cr *ColumnRef) TableRef() *TableRef {
	return &TableRef{cr.Schema, cr.Table}
}

type SequenceRef struct {
	Schema   string
	Sequence string
}

func (sr *SequenceRef) Qualified(q output.Quoter) string {
	return q.QualifyObject(sr.Schema, sr.Sequence)
}

func (sr *SequenceRef) Quoted(q output.Quoter) string {
	return q.QuoteObject(sr.Sequence)
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

func (tr *TypeRef) Qualified(q output.Quoter) string {
	if tr.Schema == "" {
		// in the case of builtin names like `text`, there is no schema and we should not quote it
		return tr.Type
	}
	return q.QualifyObject(tr.Schema, tr.Type)
}

func (tr *TypeRef) Quoted(q output.Quoter) string {
	if tr.Schema == "" {
		// in the case of builtin names like `text`, there is no schema and we should not quote it
		return tr.Type
	}
	return q.QuoteObject(tr.Type)
}

type TriggerRef struct {
	Schema  string
	Trigger string
}

func (tr *TriggerRef) Qualified(q output.Quoter) string {
	return q.QualifyObject(tr.Schema, tr.Trigger)
}

func (tr *TriggerRef) Quoted(q output.Quoter) string {
	return q.QuoteObject(tr.Trigger)
}

type ViewRef struct {
	Schema string
	View   string
}

func (vr *ViewRef) Qualified(q output.Quoter) string {
	return q.QualifyObject(vr.Schema, vr.View)
}

func (vr *ViewRef) Quoted(q output.Quoter) string {
	return q.QuoteObject(vr.View)
}

type IndexRef struct {
	Schema string
	Index  string
}

func (ir *IndexRef) Qualified(q output.Quoter) string {
	return q.QualifyObject(ir.Schema, ir.Index)
}

func (ir *IndexRef) Quoted(q output.Quoter) string {
	return q.QuoteObject(ir.Index)
}

type FunctionRef struct {
	Schema   string
	Function string
	Params   []string
}

func (fr *FunctionRef) Qualified(q output.Quoter) string {
	return fmt.Sprintf("%s(%s)", q.QualifyObject(fr.Schema, fr.Function), strings.Join(fr.Params, ", "))
}

func (fr *FunctionRef) Quoted(q output.Quoter) string {
	return q.QuoteObject(fr.Function)
}
