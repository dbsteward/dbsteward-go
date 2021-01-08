package lib

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib/model"
)

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

func (self *SqlParser) ParseQualifiedTableName(table string) QualifiedTable {
	// TODO(go,core) need to properly parse possible quoted names
	if strings.Contains(table, ".") {
		parts := strings.SplitN(table, ".", 2)
		return QualifiedTable{parts[0], parts[1]}
	}
	return QualifiedTable{GlobalDBX.GetDefaultSchema().Name, table}
}

func (self *SqlParser) GetSchemaName(name string) string {
	return self.ParseQualifiedTableName(name).Schema
}

func (self *SqlParser) GetObjectName(name string, def *model.Definition) string {
	// TODO(go,core)
	return ""
}
