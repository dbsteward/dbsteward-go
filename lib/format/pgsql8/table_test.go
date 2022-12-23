package pgsql8_test

import (
	"testing"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/stretchr/testify/assert"
)

func TestTable_GetCreationSql_TableOptions(t *testing.T) {
	schema := &model.Schema{
		Name: "public",
		Tables: []*model.Table{
			&model.Table{
				Name:        "test",
				PrimaryKey:  []string{"id"},
				Description: "test description",
				Columns: []*model.Column{
					{Name: "id", Type: "int"},
					{Name: "foo", Type: "int"},
				},
				TableOptions: []*model.TableOption{
					&model.TableOption{
						SqlFormat: model.SqlFormatPgsql8,
						Name:      "tablespace",
						Value:     "schmableschpace",
					},
					&model.TableOption{
						SqlFormat: model.SqlFormatPgsql8,
						Name:      "with",
						Value:     "(oids=true,fillfactor=70)",
					},
					&model.TableOption{
						SqlFormat: model.SqlFormatMysql5,
						Name:      "auto_increment",
						Value:     "5",
					},
				},
			},
		},
	}

	ddl := pgsql8.GlobalTable.GetCreationSql(schema, schema.Tables[0])
	assert.Equal(t, []output.ToSql{
		&sql.TableCreate{
			Table: sql.TableRef{"public", "test"},
			Columns: []sql.ColumnDefinition{
				{Name: "id", Type: sql.TypeRef{"", "int"}},
				{Name: "foo", Type: sql.TypeRef{"", "int"}},
			},
			OtherOptions: []sql.TableCreateOption{
				{Option: "tablespace", Value: "schmableschpace"},
				{Option: "with", Value: "(oids=true,fillfactor=70)"},
			},
		},
		&sql.TableSetComment{
			Table:   sql.TableRef{"public", "test"},
			Comment: "test description",
		},
	}, ddl)
}
