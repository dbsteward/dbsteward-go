package pgsql8

import (
	"testing"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"

	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/stretchr/testify/assert"

	"github.com/dbsteward/dbsteward/lib/ir"
)

func TestDiffTables_GetDataSql_EscapeCharacters(t *testing.T) {
	// NOTE: v1 uses this test to validate that data values (particularly strings) are encoded correctly
	// in v2, that is relegated to the sql generation layer, which is tested separately. this test
	// now functions to validate that data values are transferred to the correct sql values
	schema := &ir.Schema{
		Name: "public",
		Tables: []*ir.Table{
			&ir.Table{
				Name:       "i_test",
				PrimaryKey: []string{"pk"},
				Columns: []*ir.Column{
					{Name: "pk", Type: "int"},
					{Name: "col1", Type: "char(10)"},
				},
				Rows: &ir.DataRows{
					Columns: []string{"pk", "col1"},
					Rows: []*ir.DataRow{
						&ir.DataRow{
							Columns: []*ir.DataCol{
								{Text: "1"},
								{Text: "hi"},
							},
						},
					},
				},
			},
		},
	}

	ddl := getCreateDataSql(nil, nil, schema, schema.Tables[0])
	assert.Equal(t, []output.ToSql{
		&sql.DataInsert{
			Table:   sql.TableRef{"public", "i_test"},
			Columns: []string{"pk", "col1"},
			Values: []sql.ToSqlValue{
				&sql.TypedValue{"int", "1", false},
				&sql.TypedValue{"char(10)", "hi", false},
			},
		},
	}, ddl)
}
