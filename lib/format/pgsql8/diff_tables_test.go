package pgsql8_test

import (
	"strings"
	"testing"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/stretchr/testify/assert"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sqltest"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

func TestDiffTables_DiffTables_ColumnCaseChange(t *testing.T) {
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

func diffTablesCommon(oldSchema, newSchema *model.Schema) ([]output.ToSql, []output.ToSql, error) {
	oldDoc := &model.Definition{
		Schemas: []*model.Schema{oldSchema},
	}
	newDoc := &model.Definition{
		Schemas: []*model.Schema{newSchema},
	}
	setOldNewDocs(oldDoc, newDoc)
	ofs1 := &sqltest.RecordingOfs{
		StripComments: true,
		Sql:           []output.ToSql{},
	}
	ofs3 := &sqltest.RecordingOfs{
		StripComments: true,
		Sql:           []output.ToSql{},
	}

	// note: v1 only used DiffTables, v2 split into CreateTables+DiffTables
	dt := pgsql8.NewDiffTables()
	err := dt.CreateTables(ofs1, oldSchema, newSchema)
	if err != nil {
		return ofs1.Sql, ofs3.Sql, err
	}

	err = dt.DiffTables(ofs1, ofs3, oldSchema, newSchema)
	return ofs1.Sql, ofs3.Sql, err
}
