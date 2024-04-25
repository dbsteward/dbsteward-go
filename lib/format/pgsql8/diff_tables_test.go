package pgsql8

import (
	"strings"
	"testing"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/stretchr/testify/assert"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/pgtestutil"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

func TestDiffTables_DiffTables_ColumnCaseChange(t *testing.T) {
	defer resetGlobalDBSteward()
	lower := &ir.Schema{
		Name: "test0",
		Tables: []*ir.Table{
			&ir.Table{
				Name: "table",
				Columns: []*ir.Column{
					{Name: "column", Type: "int"},
				},
			},
		},
	}

	upperWithoutOldName := &ir.Schema{
		Name: "test0",
		Tables: []*ir.Table{
			&ir.Table{
				Name: "table",
				Columns: []*ir.Column{
					{Name: "CoLuMn", Type: "int"},
				},
			},
		},
	}

	upperWithOldName := &ir.Schema{
		Name: "test0",
		Tables: []*ir.Table{
			&ir.Table{
				Name: "table",
				Columns: []*ir.Column{
					{Name: "CoLuMn", Type: "int", OldColumnName: "column"},
				},
			},
		},
	}

	lib.GlobalDBSteward.IgnoreOldNames = false

	// when quoting is off, a change in case is a no-op
	lib.GlobalDBSteward.QuoteAllNames = false
	lib.GlobalDBSteward.QuoteColumnNames = false
	ddl1, ddl3, err := diffTablesCommon(lower, upperWithoutOldName)
	assert.Equal(t, []output.ToSql{}, ddl1)
	assert.Equal(t, []output.ToSql{}, ddl3)
	assert.NoError(t, err)

	// when quoting is on, a change in case results in a rename, if there's an oldname
	lib.GlobalDBSteward.QuoteAllNames = true
	lib.GlobalDBSteward.QuoteColumnNames = true
	ddl1, ddl3, err = diffTablesCommon(lower, upperWithOldName)
	assert.Equal(t, []output.ToSql{
		&sql.ColumnRename{
			Column:  sql.ColumnRef{"test0", "table", "column"},
			NewName: "CoLuMn",
		},
	}, ddl1)
	assert.Equal(t, []output.ToSql{}, ddl3)
	assert.NoError(t, err)

	// but, if oldColumnName is not given when doing case sensitive renames, it should error
	_, _, err = diffTablesCommon(lower, upperWithoutOldName)
	if assert.Error(t, err) {
		assert.Contains(t, strings.ToLower(err.Error()), "ambiguous operation")
	}
}

func TestDiffTables_DiffTables_TableOptions_NoChange(t *testing.T) {
	schema := &ir.Schema{
		Name: "public",
		Tables: []*ir.Table{
			&ir.Table{
				Name:       "test",
				PrimaryKey: []string{"a"},
				TableOptions: []*ir.TableOption{
					&ir.TableOption{
						SqlFormat: ir.SqlFormatPgsql8,
						Name:      "tablespace",
						Value:     "foo",
					},
				},
			},
		},
	}

	ddl1, ddl3, err := diffTablesCommon(schema, schema)
	assert.Equal(t, []output.ToSql{}, ddl1)
	assert.Equal(t, []output.ToSql{}, ddl3)
	assert.NoError(t, err)
}
func TestDiffTables_DiffTables_TableOptions_AddWith(t *testing.T) {
	oldSchema := &ir.Schema{
		Name: "public",
		Tables: []*ir.Table{
			&ir.Table{
				Name:         "test",
				PrimaryKey:   []string{"a"},
				TableOptions: []*ir.TableOption{},
			},
		},
	}
	newSchema := &ir.Schema{
		Name: "public",
		Tables: []*ir.Table{
			&ir.Table{
				Name:       "test",
				PrimaryKey: []string{"a"},
				TableOptions: []*ir.TableOption{
					&ir.TableOption{
						SqlFormat: ir.SqlFormatPgsql8,
						Name:      "with",
						Value:     "(fillfactor=70)",
					},
				},
			},
		},
	}

	ddl1, ddl3, err := diffTablesCommon(oldSchema, newSchema)
	assert.Equal(t, []output.ToSql{
		sql.NewTableAlter(
			sql.TableRef{"public", "test"},
			&sql.TableAlterPartWithoutOids{},
			&sql.TableAlterPartSetStorageParams{map[string]string{
				"fillfactor": "70",
			}},
		),
	}, ddl1)
	assert.Equal(t, []output.ToSql{}, ddl3)
	assert.NoError(t, err)
}
func TestDiffTables_DiffTables_TableOptions_AlterWith(t *testing.T) {
	oldSchema := &ir.Schema{
		Name: "public",
		Tables: []*ir.Table{
			&ir.Table{
				Name:       "test",
				PrimaryKey: []string{"a"},
				TableOptions: []*ir.TableOption{
					&ir.TableOption{
						SqlFormat: ir.SqlFormatPgsql8,
						Name:      "with",
						Value:     "(oids=true,fillfactor=70)",
					},
				},
			},
		},
	}

	// remove oids=true, change fillfactor to 80
	newSchema := &ir.Schema{
		Name: "public",
		Tables: []*ir.Table{
			&ir.Table{
				Name:       "test",
				PrimaryKey: []string{"a"},
				TableOptions: []*ir.TableOption{
					&ir.TableOption{
						SqlFormat: ir.SqlFormatPgsql8,
						Name:      "with",
						Value:     "(fillfactor=80)",
					},
				},
			},
		},
	}

	ddl1, ddl3, err := diffTablesCommon(oldSchema, newSchema)
	assert.Equal(t, []output.ToSql{
		sql.NewTableAlter(
			sql.TableRef{"public", "test"},
			&sql.TableAlterPartWithoutOids{},
			&sql.TableAlterPartSetStorageParams{map[string]string{
				"fillfactor": "80",
			}},
		),
	}, ddl1)
	assert.Equal(t, []output.ToSql{}, ddl3)
	assert.NoError(t, err)
}
func TestDiffTables_DiffTables_TableOptions_AddTablespaceAlterWith(t *testing.T) {
	oldSchema := &ir.Schema{
		Name: "public",
		Tables: []*ir.Table{
			&ir.Table{
				Name:       "test",
				PrimaryKey: []string{"a"},
				TableOptions: []*ir.TableOption{
					&ir.TableOption{
						SqlFormat: ir.SqlFormatPgsql8,
						Name:      "with",
						Value:     "(oids=true,fillfactor=70)",
					},
				},
			},
		},
	}

	// remove oids=true, change fillfactor to 80, add tablespace
	newSchema := &ir.Schema{
		Name: "public",
		Tables: []*ir.Table{
			&ir.Table{
				Name:       "test",
				PrimaryKey: []string{"a"},
				TableOptions: []*ir.TableOption{
					&ir.TableOption{
						SqlFormat: ir.SqlFormatPgsql8,
						Name:      "with",
						Value:     "(fillfactor=80)",
					},
					&ir.TableOption{
						SqlFormat: ir.SqlFormatPgsql8,
						Name:      "tablespace",
						Value:     "foo",
					},
				},
			},
		},
	}

	ddl1, ddl3, err := diffTablesCommon(oldSchema, newSchema)
	assert.Equal(t, []output.ToSql{
		&sql.TableMoveTablespaceIndexes{
			Table:      sql.TableRef{"public", "test"},
			Tablespace: "foo",
		},
		sql.NewTableAlter(
			sql.TableRef{"public", "test"},
			&sql.TableAlterPartSetTablespace{"foo"},
			&sql.TableAlterPartWithoutOids{},
			&sql.TableAlterPartSetStorageParams{map[string]string{
				"fillfactor": "80",
			}},
		),
	}, ddl1)
	assert.Equal(t, []output.ToSql{}, ddl3)
	assert.NoError(t, err)
}
func TestDiffTables_DiffTables_TableOptions_DropTablespace(t *testing.T) {
	oldSchema := &ir.Schema{
		Name: "public",
		Tables: []*ir.Table{
			&ir.Table{
				Name:       "test",
				PrimaryKey: []string{"a"},
				TableOptions: []*ir.TableOption{
					&ir.TableOption{
						SqlFormat: ir.SqlFormatPgsql8,
						Name:      "with",
						Value:     "(oids=false,fillfactor=70)",
					},
					&ir.TableOption{
						SqlFormat: ir.SqlFormatPgsql8,
						Name:      "tablespace",
						Value:     "foo",
					},
				},
			},
		},
	}

	// remove oids=true, change fillfactor to 80, add tablespace
	newSchema := &ir.Schema{
		Name: "public",
		Tables: []*ir.Table{
			&ir.Table{
				Name:       "test",
				PrimaryKey: []string{"a"},
				TableOptions: []*ir.TableOption{
					&ir.TableOption{
						SqlFormat: ir.SqlFormatPgsql8,
						Name:      "with",
						Value:     "(oids=false,fillfactor=70)",
					},
				},
			},
		},
	}

	ddl1, ddl3, err := diffTablesCommon(oldSchema, newSchema)
	assert.Equal(t, []output.ToSql{
		&sql.TableResetTablespace{
			Table: sql.TableRef{"public", "test"},
		},
	}, ddl1)
	assert.Equal(t, []output.ToSql{}, ddl3)
	assert.NoError(t, err)
}

func TestDiffTables_GetDeleteCreateDataSql_AddSerialColumn(t *testing.T) {
	oldSchema := &ir.Schema{
		Name: "test",
		Tables: []*ir.Table{
			&ir.Table{
				Name:       "serial_test",
				PrimaryKey: []string{"test_string"},
				Columns: []*ir.Column{
					{Name: "test_string", Type: "text"},
					{Name: "test_number", Type: "integer"},
				},
				Rows: &ir.DataRows{
					Columns: []string{"test_string", "test_number"},
					// NOTE original test used tabrows, but that's already been expanded by this point
					Rows: []*ir.DataRow{
						{Columns: []*ir.DataCol{{Text: "testtest"}, {Text: "12345"}}},
					},
				},
			},
		},
	}
	newSchema := &ir.Schema{
		Name: "test",
		Tables: []*ir.Table{
			&ir.Table{
				Name:       "serial_test",
				PrimaryKey: []string{"test_string"},
				Columns: []*ir.Column{
					{Name: "test_serial", Type: "serial"},
					{Name: "test_string", Type: "text"},
					{Name: "test_number", Type: "integer"},
				},
				Rows: &ir.DataRows{
					Columns: []string{"test_serial", "test_string", "test_number"},
					// NOTE original test used tabrows, but that's already been expanded by this point
					Rows: []*ir.DataRow{
						{Columns: []*ir.DataCol{{Text: "1"}, {Text: "testtest"}, {Text: "12345"}}},
					},
				},
			},
		},
	}

	delddl := getDeleteDataSql(oldSchema, oldSchema.Tables[0], newSchema, newSchema.Tables[0])
	addddl := getCreateDataSql(oldSchema, oldSchema.Tables[0], newSchema, newSchema.Tables[0])
	assert.Equal(t, []output.ToSql{}, delddl)
	assert.Equal(t, []output.ToSql{
		&sql.DataUpdate{
			Table:          sql.TableRef{"test", "serial_test"},
			UpdatedColumns: []string{"test_serial"},
			UpdatedValues:  []sql.ToSqlValue{&sql.TypedValue{"serial", "1", false}},
			KeyColumns:     []string{"test_string"},
			KeyValues:      []sql.ToSqlValue{&sql.TypedValue{"text", "testtest", false}},
		},
	}, addddl)
}

func diffTablesCommon(oldSchema, newSchema *ir.Schema) ([]output.ToSql, []output.ToSql, error) {
	oldDoc := &ir.Definition{
		Schemas: []*ir.Schema{oldSchema},
	}
	newDoc := &ir.Definition{
		Schemas: []*ir.Schema{newSchema},
	}
	setOldNewDocs(oldDoc, newDoc)
	ofs1 := &pgtestutil.RecordingOfs{
		StripComments: true,
		Sql:           []output.ToSql{},
	}
	ofs3 := &pgtestutil.RecordingOfs{
		StripComments: true,
		Sql:           []output.ToSql{},
	}

	// note: v1 only used DiffTables, v2 split into CreateTables+DiffTables
	err := createTables(ofs1, oldSchema, newSchema)
	if err != nil {
		return ofs1.Sql, ofs3.Sql, err
	}

	err = diffTables(ofs1, ofs3, oldSchema, newSchema)
	return ofs1.Sql, ofs3.Sql, err
}
