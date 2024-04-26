package output

import "fmt"

// DDLStatement for tracking individual DDL statements
type DDLStatement struct {
	Comment   string
	Statement string
}

func NewSegmenter(q Quoter) *Segmenter {
	return &Segmenter{quoter: q}
}

// Segmenter is a output file segmenter that holds everything
// internally in arrays and the returns the properly ordered
// list from AllStatements()
type Segmenter struct {
	quoter Quoter
	header []DDLStatement
	body   []DDLStatement
	footer []DDLStatement
	final  []DDLStatement
}

// Close compiles the different parts into a single list of
// statements. It appears as if this is not consistely called
// by code that uses output file segmenters.
func (s *Segmenter) Close() {
	s.final = append(s.header, s.body...)
	s.final = append(s.final, s.footer...)
	s.header = nil
	s.body = nil
	s.footer = nil
}

// SetHeader removes any previous header statements and
// starts the header fresh
func (s *Segmenter) SetHeader(format string, args ...interface{}) {
	s.header = []DDLStatement{{Statement: fmt.Sprintf(format, args...)}}
}

// AppendHeader adds a new DDL statement to the header
func (s *Segmenter) AppendHeader(format string, args ...interface{}) {
	s.header = append(s.header, DDLStatement{Statement: fmt.Sprintf(format, args...)})
}

// AppendFooter adds a new DDL statement to the footer
func (s *Segmenter) AppendFooter(format string, args ...interface{}) {
	s.footer = append(s.footer, DDLStatement{Statement: fmt.Sprintf(format, args...)})
}

// Write appens a new DDL statement to the body
func (s *Segmenter) Write(format string, args ...interface{}) {
	s.body = append(s.body, DDLStatement{Statement: fmt.Sprintf(format, args...)})
}

// WriteSql appends the output of ToSql() from each generator
// to the body in turn
func (s *Segmenter) WriteSql(generators ...ToSql) {
	for _, g := range generators {
		s.body = append(s.body, DDLStatement{Statement: g.ToSql(s.quoter)})
	}
}

// AllStatements compiles the 3 parts in a single list if it
// wasn't previously done, then returns that list.
func (s *Segmenter) AllStatements() []DDLStatement {
	if len(s.final) == 0 {
		s.Close()
	}
	return s.final
}
