package pgsql8

import (
	"log/slog"
	"testing"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/stretchr/testify/assert"
)

func TestTable_GetCreationSql_TableOptions(t *testing.T) {
	schema := &ir.Schema{
		Name: "public",
		Tables: []*ir.Table{
			&ir.Table{
				Name:        "test",
				PrimaryKey:  []string{"id"},
				Description: "test description",
				Columns: []*ir.Column{
					{Name: "id", Type: "int"},
					{Name: "foo", Type: "int"},
				},
				TableOptions: []*ir.TableOption{
					&ir.TableOption{
						SqlFormat: ir.SqlFormatPgsql8,
						Name:      "tablespace",
						Value:     "schmableschpace",
					},
					&ir.TableOption{
						SqlFormat: ir.SqlFormatPgsql8,
						Name:      "with",
						Value:     "(oids=true,fillfactor=70)",
					},
					&ir.TableOption{
						SqlFormat: ir.SqlFormatMysql5,
						Name:      "auto_increment",
						Value:     "5",
					},
				},
			},
		},
	}

	ddl, err := getCreateTableSql(slog.Default(), schema, schema.Tables[0])
	if err != nil {
		t.Fatal(err)
	}
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
