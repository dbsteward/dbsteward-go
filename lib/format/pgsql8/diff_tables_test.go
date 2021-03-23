package pgsql8_test

import (
	"strings"
	"testing"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/stretchr/testify/assert"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/pgtestutil"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

func TestDiffTables_DiffTables_ColumnCaseChange(t *testing.T) {
	defer resetGlobalDBSteward()
	lower := &model.Schema{
		Name: "test0",
		Tables: []*model.Table{
			&model.Table{
				Name: "table",
				Columns: []*model.Column{
					{Name: "column", Type: "int"},
				},
			},
		},
	}

	upperWithoutOldName := &model.Schema{
		Name: "test0",
		Tables: []*model.Table{
			&model.Table{
				Name: "table",
				Columns: []*model.Column{
					{Name: "CoLuMn", Type: "int"},
				},
			},
		},
	}

	upperWithOldName := &model.Schema{
		Name: "test0",
		Tables: []*model.Table{
			&model.Table{
				Name: "table",
				Columns: []*model.Column{
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
	schema := &model.Schema{
		Name: "public",
		Tables: []*model.Table{
			&model.Table{
				Name:       "test",
				PrimaryKey: model.DelimitedList{"a"},
				TableOptions: []*model.TableOption{
					&model.TableOption{
						SqlFormat: model.SqlFormatPgsql8,
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
	oldSchema := &model.Schema{
		Name: "public",
		Tables: []*model.Table{
			&model.Table{
				Name:         "test",
				PrimaryKey:   model.DelimitedList{"a"},
				TableOptions: []*model.TableOption{},
			},
		},
	}
	newSchema := &model.Schema{
		Name: "public",
		Tables: []*model.Table{
			&model.Table{
				Name:       "test",
				PrimaryKey: model.DelimitedList{"a"},
				TableOptions: []*model.TableOption{
					&model.TableOption{
						SqlFormat: model.SqlFormatPgsql8,
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
	oldSchema := &model.Schema{
		Name: "public",
		Tables: []*model.Table{
			&model.Table{
				Name:       "test",
				PrimaryKey: model.DelimitedList{"a"},
				TableOptions: []*model.TableOption{
					&model.TableOption{
						SqlFormat: model.SqlFormatPgsql8,
						Name:      "with",
						Value:     "(oids=true,fillfactor=70)",
					},
				},
			},
		},
	}

	// remove oids=true, change fillfactor to 80
	newSchema := &model.Schema{
		Name: "public",
		Tables: []*model.Table{
			&model.Table{
				Name:       "test",
				PrimaryKey: model.DelimitedList{"a"},
				TableOptions: []*model.TableOption{
					&model.TableOption{
						SqlFormat: model.SqlFormatPgsql8,
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
	oldSchema := &model.Schema{
		Name: "public",
		Tables: []*model.Table{
			&model.Table{
				Name:       "test",
				PrimaryKey: model.DelimitedList{"a"},
				TableOptions: []*model.TableOption{
					&model.TableOption{
						SqlFormat: model.SqlFormatPgsql8,
						Name:      "with",
						Value:     "(oids=true,fillfactor=70)",
					},
				},
			},
		},
	}

	// remove oids=true, change fillfactor to 80, add tablespace
	newSchema := &model.Schema{
		Name: "public",
		Tables: []*model.Table{
			&model.Table{
				Name:       "test",
				PrimaryKey: model.DelimitedList{"a"},
				TableOptions: []*model.TableOption{
					&model.TableOption{
						SqlFormat: model.SqlFormatPgsql8,
						Name:      "with",
						Value:     "(fillfactor=80)",
					},
					&model.TableOption{
						SqlFormat: model.SqlFormatPgsql8,
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
	oldSchema := &model.Schema{
		Name: "public",
		Tables: []*model.Table{
			&model.Table{
				Name:       "test",
				PrimaryKey: model.DelimitedList{"a"},
				TableOptions: []*model.TableOption{
					&model.TableOption{
						SqlFormat: model.SqlFormatPgsql8,
						Name:      "with",
						Value:     "(oids=false,fillfactor=70)",
					},
					&model.TableOption{
						SqlFormat: model.SqlFormatPgsql8,
						Name:      "tablespace",
						Value:     "foo",
					},
				},
			},
		},
	}

	// remove oids=true, change fillfactor to 80, add tablespace
	newSchema := &model.Schema{
		Name: "public",
		Tables: []*model.Table{
			&model.Table{
				Name:       "test",
				PrimaryKey: model.DelimitedList{"a"},
				TableOptions: []*model.TableOption{
					&model.TableOption{
						SqlFormat: model.SqlFormatPgsql8,
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
	oldSchema := &model.Schema{
		Name: "test",
		Tables: []*model.Table{
			&model.Table{
				Name:       "serial_test",
				PrimaryKey: model.DelimitedList{"test_string"},
				Columns: []*model.Column{
					{Name: "test_string", Type: "text"},
					{Name: "test_number", Type: "integer"},
				},
				Rows: &model.DataRows{
					Columns: model.DelimitedList{"test_string", "test_number"},
					// NOTE original test used tabrows, but that's already been expanded by this point
					Rows: []*model.DataRow{
						{Columns: []*model.DataCol{{Text: "testtest"}, {Text: "12345"}}},
					},
				},
			},
		},
	}
	newSchema := &model.Schema{
		Name: "test",
		Tables: []*model.Table{
			&model.Table{
				Name:       "serial_test",
				PrimaryKey: model.DelimitedList{"test_string"},
				Columns: []*model.Column{
					{Name: "test_serial", Type: "serial"},
					{Name: "test_string", Type: "text"},
					{Name: "test_number", Type: "integer"},
				},
				Rows: &model.DataRows{
					Columns: model.DelimitedList{"test_serial", "test_string", "test_number"},
					// NOTE original test used tabrows, but that's already been expanded by this point
					Rows: []*model.DataRow{
						{Columns: []*model.DataCol{{Text: "1"}, {Text: "testtest"}, {Text: "12345"}}},
					},
				},
			},
		},
	}

	delddl := pgsql8.GlobalDiffTables.GetDeleteDataSql(oldSchema, oldSchema.Tables[0], newSchema, newSchema.Tables[0])
	addddl := pgsql8.GlobalDiffTables.GetCreateDataSql(oldSchema, oldSchema.Tables[0], newSchema, newSchema.Tables[0])
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

func diffTablesCommon(oldSchema, newSchema *model.Schema) ([]output.ToSql, []output.ToSql, error) {
	oldDoc := &model.Definition{
		Schemas: []*model.Schema{oldSchema},
	}
	newDoc := &model.Definition{
		Schemas: []*model.Schema{newSchema},
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
	dt := pgsql8.GlobalDiffTables
	err := dt.CreateTables(ofs1, oldSchema, newSchema)
	if err != nil {
		return ofs1.Sql, ofs3.Sql, err
	}

	err = dt.DiffTables(ofs1, ofs3, oldSchema, newSchema)
	return ofs1.Sql, ofs3.Sql, err
}
