package lib

import (
	"strings"
)

// TODO(go,pgsql) move this to pgsql8/sql

type QualifiedTable struct {
	Schema string
	Table  string
}

func ParseQualifiedTableName(table string) QualifiedTable {
	// TODO(go,core) need to properly parse possible quoted names
	if strings.Contains(table, ".") {
		parts := strings.SplitN(table, ".", 2)
		return QualifiedTable{parts[0], parts[1]}
	}
	return QualifiedTable{"public", table}
}
