package sqltest

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"

	"github.com/dbsteward/dbsteward/lib/output"
)

type RecordingOfs struct {
	StripComments bool
	Sql           []output.ToSql
	Writes        []string
	Header        string
	Footer        string
}

func (self *RecordingOfs) Close() {
}
func (self *RecordingOfs) SetHeader(format string, args ...interface{}) {
	self.Header = fmt.Sprintf(format, args...)
}
func (self *RecordingOfs) AppendHeader(format string, args ...interface{}) {
	self.Header += fmt.Sprintf(format, args...)
}
func (self *RecordingOfs) AppendFooter(format string, args ...interface{}) {
	self.Footer = fmt.Sprintf(format, args...)
}
func (self *RecordingOfs) Write(format string, args ...interface{}) {
	// TODO(go,nth) if self.StripComments
	self.Writes = append(self.Writes, fmt.Sprintf(format, args...))
}
func (self *RecordingOfs) WriteSql(stmts ...output.ToSql) {
	for _, stmt := range stmts {
		if self.StripComments {
			switch t := stmt.(type) {
			case *sql.Annotated:
				stmt = t.Wrapped
			case *sql.Comment:
				continue
			}
		}
		self.Sql = append(self.Sql, stmt)
	}
}
