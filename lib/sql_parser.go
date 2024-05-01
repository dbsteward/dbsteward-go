package lib

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib/ir"
)

// TODO(go,pgsql) move this to pgsql8/sql

type QualifiedTable struct {
	Schema string
	Table  string
}

var GlobalSqlParser *SqlParser = NewSqlParser()

type SqlParser struct {
}

func NewSqlParser() *SqlParser {
	return &SqlParser{}
}

func (parser *SqlParser) ParseQualifiedTableName(table string) QualifiedTable {
	// TODO(go,core) need to properly parse possible quoted names
	if strings.Contains(table, ".") {
		parts := strings.SplitN(table, ".", 2)
		return QualifiedTable{parts[0], parts[1]}
	}
	return QualifiedTable{"public", table}
}

func (parser *SqlParser) GetSchemaName(name string) string {
	return parser.ParseQualifiedTableName(name).Schema
}

func (parser *SqlParser) GetObjectName(name string, def *ir.Definition) string {
	// TODO(go,core)
	return ""
}
