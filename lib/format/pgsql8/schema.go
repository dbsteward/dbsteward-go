package pgsql8

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

var GlobalSchema *Schema = NewSchema()

type Schema struct {
}

func NewSchema() *Schema {
	return &Schema{}
}

func (self *Schema) GetCreationSql(schema *model.Schema) []output.ToSql {
	// don't create the public schema
	if strings.EqualFold(schema.Name, "public") {
		return nil
	}

	ddl := []output.ToSql{
		&sql.SchemaCreate{schema.Name},
	}

	if schema.Owner != "" {
		owner := lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, schema.Owner)
		ddl = append(ddl, &sql.SchemaAlterOwner{schema.Name, owner})
	}

	if schema.Description != "" {
		ddl = append(ddl, &sql.SchemaSetComment{schema.Name, schema.Description})
	}

	return ddl
}
