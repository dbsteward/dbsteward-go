package pgsql8

import (
	"testing"

	"github.com/dbsteward/dbsteward/lib"
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
			{
				Name:       "i_test",
				PrimaryKey: []string{"pk"},
				Columns: []*ir.Column{
					{Name: "pk", Type: "int"},
					{Name: "col1", Type: "char(10)"},
				},
				Rows: &ir.DataRows{
					Columns: []string{"pk", "col1"},
					Rows: []*ir.DataRow{
						{
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
	dbs := lib.NewDBSteward()
	dbs.NewDatabase = &ir.Definition{
		Schemas: []*ir.Schema{schema},
	}
	ops := NewOperations(dbs).(*Operations)
	ddl, err := getCreateDataSql(ops, nil, nil, schema, schema.Tables[0])
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, []output.ToSql{
		&sql.DataInsert{
			Table:   sql.TableRef{Schema: "public", Table: "i_test"},
			Columns: []string{"pk", "col1"},
			Values: []sql.ToSqlValue{
				&sql.TypedValue{Type: "int", Value: "1", IsNull: false},
				&sql.TypedValue{Type: "char(10)", Value: "hi", IsNull: false},
			},
		},
	}, ddl)
}
