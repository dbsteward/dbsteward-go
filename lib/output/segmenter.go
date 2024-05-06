package output

import (
	"fmt"
)

const CommentLinePrefix = "--"

type ToSql interface {
	ToSql(Quoter) string
}

type AnnotatedSQL interface {
	StripAnnotation() ToSql
}

type SQLComment interface {
	Comment() string
}

func NewRawSQL(format string, args ...interface{}) rawSQL {
	return rawSQL(fmt.Sprintf(format, args...))
}

type rawSQL string

func (c rawSQL) ToSql(q Quoter) string {
	return string(c)
}

type Quoter interface {
	QuoteSchema(schema string) string
	QuoteTable(table string) string
	QuoteColumn(column string) string
	QuoteRole(role string) string
	QuoteObject(obj string) string
	QualifyTable(schema, table string) string
	QualifyObject(schema, obj string) string
	QualifyColumn(schema, table, column string) string
	LiteralString(value string) string
	LiteralValue(datatype, value string, isNull bool) string
}

type OutputFileSegmenter interface {
	Close() error
	SetHeader(ToSql) error
	AppendHeader(ToSql) error
	AppendFooter(ToSql) error
	WriteSql(...ToSql) error
	// This is a hack to avoid a jillion adjustments to the code
	MustWriteSql([]ToSql, error)
}

// DDLStatement for tracking individual DDL statements
type DDLStatement struct {
	Comment   string
	Statement string
}

func NewSegmenter(q Quoter) *Segmenter {
	return &Segmenter{quoter: q}
}

func NewAnnotationStrippingSegmenter(q Quoter) *Segmenter {
	return &Segmenter{
		stripAnnotations: true,
		quoter:           q,
	}
}

// Segmenter is a output file segmenter that holds everything
// internally in arrays and the returns the properly ordered
// list from AllStatements()
type Segmenter struct {
	stripAnnotations bool
	quoter           Quoter
	Header           []ToSql
	Body             []ToSql
	Footer           []ToSql
	final            []ToSql
}

func (s *Segmenter) stripAnnotation(m ToSql) ToSql {
	if s.stripAnnotations {
		if mm, isAnnotated := m.(AnnotatedSQL); isAnnotated {
			m = mm.StripAnnotation()
		}
		if _, isComment := m.(SQLComment); isComment {
			return nil
		}
	}
	return m
}

func (s *Segmenter) stripMultipleAnnotation(m ...ToSql) []ToSql {
	if s.stripAnnotations {
		var rv []ToSql
		for _, mm := range m {
			if mm, isAnnotated := mm.(AnnotatedSQL); isAnnotated {
				rv = append(rv, mm.StripAnnotation())
				continue
			}
			if _, isComment := mm.(SQLComment); !isComment {
				rv = append(rv, mm)
			}
		}
		return rv
	}
	return m
}

// Close compiles the different parts into a single list of
// statements. It appears as if this is not consistely called
// by code that uses output file segmenters.
func (s *Segmenter) Close() error {
	s.final = append(s.Header, s.Body...)
	s.final = append(s.final, s.Footer...)
	s.Header = nil
	s.Body = nil
	s.Footer = nil
	return nil
}

// SetHeader removes any previous header statements and
// starts the header fresh
func (s *Segmenter) SetHeader(stmt ToSql) error {
	s.Header = []ToSql{s.stripAnnotation(stmt)}
	return nil
}

// AppendHeader adds a new DDL statement to the header
func (s *Segmenter) AppendHeader(stmt ToSql) error {
	if stmt == nil {
		return nil
	}
	s.Header = append(s.Header, s.stripAnnotation(stmt))
	return nil
}

// AppendFooter adds a new DDL statement to the footer
func (s *Segmenter) AppendFooter(stmt ToSql) error {
	if stmt == nil {
		return nil
	}
	s.Footer = append(s.Footer, s.stripAnnotation(stmt))
	return nil
}

// WriteSql appends the output of ToSql() from each generator
// to the body in turn
func (s *Segmenter) WriteSql(generators ...ToSql) error {
	if len(generators) == 0 {
		return nil
	}
	s.Body = append(s.Body, s.stripMultipleAnnotation(generators...)...)
	return nil
}

func (ofs *Segmenter) MustWriteSql(stmts []ToSql, err error) {
	if err != nil {
		panic(err)
	}
	err = ofs.WriteSql(stmts...)
	if err != nil {
		panic(err)
	}
}

// AllStatements compiles the 3 parts in a single list if it
// wasn't previously done, then returns that list.
func (s *Segmenter) AllStatements() []DDLStatement {
	if len(s.final) == 0 {
		_ = s.Close()
	}
	var final []DDLStatement
	for _, stmt := range s.final {
		final = append(final, DDLStatement{Statement: stmt.ToSql(s.quoter)})
	}
	return final
}
