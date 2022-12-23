package pgsql8_test

import (
	"testing"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/stretchr/testify/assert"

	"github.com/dbsteward/dbsteward/lib/model"
)

func TestDiffTables_GetDataSql_EscapeCharacters(t *testing.T) {
	// NOTE: v1 uses this test to validate that data values (particularly strings) are encoded correctly
	// in v2, that is relegated to the sql generation layer, which is tested separately. this test
	// now functions to validate that data values are transferred to the correct sql values
	schema := &model.Schema{
		Name: "public",
		Tables: []*model.Table{
			&model.Table{
				Name:       "i_test",
				PrimaryKey: []string{"pk"},
				Columns: []*model.Column{
					{Name: "pk", Type: "int"},
					{Name: "col1", Type: "char(10)"},
				},
				Rows: &model.DataRows{
					Columns: []string{"pk", "col1"},
					Rows: []*model.DataRow{
						&model.DataRow{
							Columns: []*model.DataCol{
								{Text: "1"},
								{Text: "hi"},
							},
						},
					},
				},
			},
		},
	}

	ddl := pgsql8.GlobalDiffTables.GetCreateDataSql(nil, nil, schema, schema.Tables[0])
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
