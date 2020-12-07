package lib

import (
	"github.com/dbsteward/dbsteward/lib/model"
)

var GlobalSqlParser *SqlParser = NewSqlParser()

type SqlParser struct {
}

func NewSqlParser() *SqlParser {
	return &SqlParser{}
}

func (self *SqlParser) GetSchemaName(name string, def *model.Definition) string {
	// TODO(go,core)
	return ""
}

func (self *SqlParser) GetObjectName(name string, def *model.Definition) string {
	// TODO(go,core)
	return ""
}
