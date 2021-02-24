package pgsql8_test

import (
	"testing"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sqltest"

	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/stretchr/testify/assert"
)

func TestDiffConstraints_DropCreate_SameToSame(t *testing.T) {
	ddl := diffConstraintsTableCommon(diffConstraintsSchemaPka, diffConstraintsSchemaPka, pgsql8.ConstraintTypeAll)
	assert.Equal(t, []output.ToSql{}, ddl)
}

func TestDiffConstraints_DropCreate_AddSome(t *testing.T) {
	ddl := diffConstraintsTableCommon(diffConstraintsSchemaPka, diffConstraintsSchemaPkaUqc, pgsql8.ConstraintTypeAll)
	assert.Equal(t, []output.ToSql{
		&sql.ConstraintCreateRaw{
			Table:          sql.TableRef{"public", "test"},
			Constraint:     "test_uqc_idx",
			ConstraintType: model.ConstraintTypeUnique,
			Definition:     "(uqc)",
		},
	}, ddl)
}

func TestDiffConstraints_DropCreate_DropSome(t *testing.T) {
	ddl := diffConstraintsTableCommon(diffConstraintsSchemaPkaUqc, diffConstraintsSchemaPka, pgsql8.ConstraintTypeAll)
	assert.Equal(t, []output.ToSql{
		&sql.ConstraintDrop{
			Table:      sql.TableRef{"public", "test"},
			Constraint: "test_uqc_idx",
		},
	}, ddl)
}

func TestDiffConstraints_DropCreate_ChangeOne(t *testing.T) {
	ddl := diffConstraintsTableCommon(diffConstraintsSchemaPka, diffConstraintsSchemaPkb, pgsql8.ConstraintTypeAll)
	assert.Equal(t, []output.ToSql{
		&sql.ConstraintDrop{
			Table:      sql.TableRef{"public", "test"},
			Constraint: "test_pkey",
		},
		&sql.ConstraintCreatePrimaryKey{
			Table:      sql.TableRef{"public", "test"},
			Constraint: "test_pkey",
			Columns:    []string{"pkb"},
		},
	}, ddl)
}

func TestDiffConstraints_DropCreate_AddSomeAndChange(t *testing.T) {
	ddl := diffConstraintsTableCommon(diffConstraintsSchemaPka, diffConstraintsSchemaPkbCfke, pgsql8.ConstraintTypeAll)
	assert.Equal(t, []output.ToSql{
		&sql.ConstraintDrop{
			Table:      sql.TableRef{"public", "test"},
			Constraint: "test_pkey",
		},
		&sql.ConstraintCreatePrimaryKey{
			Table:      sql.TableRef{"public", "test"},
			Constraint: "test_pkey",
			Columns:    []string{"pkb"},
		},
		&sql.ConstraintCreateRaw{
			Table:          sql.TableRef{"public", "test"},
			Constraint:     "test_cfke_fk",
			ConstraintType: model.ConstraintTypeForeign,
			Definition:     "(cfke) REFERENCES public.other (pka)",
		},
	}, ddl)
}

func TestDiffConstraints_DropCreate_DropSomeAndChange(t *testing.T) {
	ddl := diffConstraintsTableCommon(diffConstraintsSchemaPkaUqcIfkdCfke, diffConstraintsSchemaPkbCfke, pgsql8.ConstraintTypeAll)
	assert.Equal(t, []output.ToSql{
		&sql.ConstraintDrop{
			Table:      sql.TableRef{"public", "test"},
			Constraint: "test_pkey",
		},
		&sql.ConstraintDrop{
			Table:      sql.TableRef{"public", "test"},
			Constraint: "test_uqc_idx",
		},
		&sql.ConstraintDrop{
			Table:      sql.TableRef{"public", "test"},
			Constraint: "test_ifkd_fk",
		},
		&sql.ConstraintCreatePrimaryKey{
			Table:      sql.TableRef{"public", "test"},
			Constraint: "test_pkey",
			Columns:    []string{"pkb"},
		},
	}, ddl)
}

func TestDiffConstraints_DropCreate_ChangePrimaryKeyNameAndTable(t *testing.T) {
	oldSchema := &model.Schema{
		Name: "public",
		Tables: []*model.Table{
			&model.Table{
				Name:       "test",
				PrimaryKey: model.DelimitedList{"pka"},
				Columns: []*model.Column{
					{Name: "pka", Type: "int"},
				},
			},
		},
	}
	newSchema := &model.Schema{
		Name: "public",
		Tables: []*model.Table{
			&model.Table{
				Name:          "newtable",
				PrimaryKey:    model.DelimitedList{"pkb"},
				OldSchemaName: "public",
				OldTableName:  "test",
				Columns: []*model.Column{
					{Name: "pkb", Type: "int", OldColumnName: "pka"},
				},
			},
		},
	}

	// in psql8_diff::update_structure() when the new schema doesn't contain the old table name,
	// $new_table is set to null for the first diff_constraints_table() call, adjusted this test accordingly
	// ddl := diffConstraintsTableCommon(oldSchema, newSchema, pgsql8.ConstraintTypePrimaryKey)
	oldDoc := &model.Definition{
		Schemas: []*model.Schema{oldSchema},
	}
	newDoc := &model.Definition{
		Schemas: []*model.Schema{newSchema},
	}
	ofs := &sqltest.RecordingOfs{
		StripComments: true,
	}
	setOldNewDocs(oldDoc, newDoc)
	diffConstraints := pgsql8.GlobalDiffConstraints
	diffConstraints.DropConstraintsTable(ofs, oldSchema, oldSchema.Tables[0], newSchema, nil, pgsql8.ConstraintTypePrimaryKey)
	diffConstraints.CreateConstraintsTable(ofs, oldSchema, oldSchema.Tables[0], newSchema, newSchema.Tables[0], pgsql8.ConstraintTypePrimaryKey)

	assert.Equal(t, []output.ToSql{
		&sql.ConstraintDrop{
			Table:      sql.TableRef{"public", "newtable"},
			Constraint: "test_pkey",
		},
		&sql.ConstraintCreatePrimaryKey{
			Table:      sql.TableRef{"public", "newtable"},
			Constraint: "newtable_pkey",
			Columns:    []string{"pkb"},
		},
	}, ofs.Sql)
}

// TODO(go,pgsql) This test exists as tests/pgsql8/Pgsql8ConstraintDiffSQLTest.php testAutoIncrement() but I can't find
// any reference to dealing with `auto_increment` type flags in postgres, everything is gated behind sqlformat==mysql5
// I don't know why this works in v1, but we should figure it out

func TestDiffConstraints_DropCreate_AutoIncrement(t *testing.T) {
	t.SkipNow()

	auto := &model.Schema{
		Name: "public",
		Tables: []*model.Table{
			&model.Table{
				Name:       "test",
				PrimaryKey: model.DelimitedList{"pka"},
				Columns: []*model.Column{
					{Name: "pka", Type: "int auto_increment"},
				},
			},
		},
	}
	noAuto := &model.Schema{
		Name: "public",
		Tables: []*model.Table{
			&model.Table{
				Name:       "newtable",
				PrimaryKey: model.DelimitedList{"pka"},
				Columns: []*model.Column{
					{Name: "pka", Type: "int"},
				},
			},
		},
	}

	// auto-increment is no longer considered a constraint, but rather part of a type, and is calculated during the tables diff
	assert.Equal(t, []output.ToSql{}, diffConstraintsTableCommon(auto, auto, pgsql8.ConstraintTypeAll))
	assert.Equal(t, []output.ToSql{}, diffConstraintsTableCommon(noAuto, noAuto, pgsql8.ConstraintTypeAll))
	assert.Equal(t, []output.ToSql{}, diffConstraintsTableCommon(noAuto, auto, pgsql8.ConstraintTypePrimaryKey))
}

func TestDiffConstraints_DropCreate_ChangeColumnTypeWithFK(t *testing.T) {
	oldSchema := &model.Schema{
		Name: "public",
		Tables: []*model.Table{
			&model.Table{
				Name:       "test",
				PrimaryKey: model.DelimitedList{"pka"},
				Columns: []*model.Column{
					{Name: "pka", Type: "int"},
					{Name: "ifkd", ForeignTable: "other", ForeignColumn: "pka", ForeignKeyName: "test_ifkd_fk"},
				},
			},
			&model.Table{
				Name:       "other",
				PrimaryKey: model.DelimitedList{"pka"},
				Columns: []*model.Column{
					{Name: "pka", Type: "int"},
				},
			},
		},
	}
	// changed type of other.pka from int to text
	newSchema := &model.Schema{
		Name: "public",
		Tables: []*model.Table{
			&model.Table{
				Name:       "test",
				PrimaryKey: model.DelimitedList{"pka"},
				Columns: []*model.Column{
					{Name: "pka", Type: "int"},
					{Name: "ifkd", ForeignTable: "other", ForeignColumn: "pka", ForeignKeyName: "test_ifkd_fk"},
				},
			},
			&model.Table{
				Name:       "other",
				PrimaryKey: model.DelimitedList{"pka"},
				Columns: []*model.Column{
					{Name: "pka", Type: "text"},
				},
			},
		},
	}

	ddl := diffConstraintsTableCommon(oldSchema, newSchema, pgsql8.ConstraintTypeAll)
	assert.Equal(t, []output.ToSql{
		&sql.ConstraintDrop{
			Table:      sql.TableRef{"public", "test"},
			Constraint: "test_ifkd_fk",
		},
		&sql.ConstraintCreateForeignKey{
			Table:          sql.TableRef{"public", "test"},
			Constraint:     "test_ifkd_fk",
			LocalColumns:   []string{"ifkd"},
			ForeignTable:   sql.TableRef{"public", "other"},
			ForeignColumns: []string{"pka"},
		},
	}, ddl)
}

var diffConstraintsSchemaPka = &model.Schema{
	Name: "public",
	Tables: []*model.Table{
		&model.Table{
			Name:       "test",
			PrimaryKey: model.DelimitedList{"pka"},
			Columns: []*model.Column{
				{Name: "pka"},
				{Name: "pkb"},
				{Name: "ukc"},
				{Name: "ifkd"},
				{Name: "cfke"},
			},
		},
	},
}

var diffConstraintsSchemaPkb = &model.Schema{
	Name: "public",
	Tables: []*model.Table{
		&model.Table{
			Name:       "test",
			PrimaryKey: model.DelimitedList{"pkb"},
			Columns: []*model.Column{
				{Name: "pka"},
				{Name: "pkb"},
				{Name: "ukc"},
				{Name: "ifkd"},
				{Name: "cfke"},
			},
		},
	},
}

var diffConstraintsSchemaPkaUqc = &model.Schema{
	Name: "public",
	Tables: []*model.Table{
		&model.Table{
			Name:       "test",
			PrimaryKey: model.DelimitedList{"pka"},
			Columns: []*model.Column{
				{Name: "pka"},
				{Name: "pkb"},
				{Name: "ukc"},
				{Name: "ifkd"},
				{Name: "cfke"},
			},
			Constraints: []*model.Constraint{
				{Name: "test_uqc_idx", Type: model.ConstraintTypeUnique, Definition: "(uqc)"},
			},
		},
	},
}

var diffConstraintsSchemaPkbCfke = &model.Schema{
	Name: "public",
	Tables: []*model.Table{
		&model.Table{
			Name:       "test",
			PrimaryKey: model.DelimitedList{"pkb"},
			Columns: []*model.Column{
				{Name: "pka"},
				{Name: "pkb"},
				{Name: "ukc"},
				{Name: "ifkd"},
				{Name: "cfke"},
			},
			Constraints: []*model.Constraint{
				{Name: "test_cfke_fk", Type: model.ConstraintTypeForeign, Definition: "(cfke) REFERENCES public.other (pka)"},
			},
		},
		&model.Table{
			Name:       "other",
			PrimaryKey: model.DelimitedList{"pka"},
			Columns: []*model.Column{
				{Name: "pka"},
				{Name: "pkb"},
				{Name: "ukc"},
				{Name: "ifkd"},
				{Name: "cfke"},
			},
		},
	},
}

var diffConstraintsSchemaPkaUqcIfkdCfke = &model.Schema{
	Name: "public",
	Tables: []*model.Table{
		&model.Table{
			Name:       "test",
			PrimaryKey: model.DelimitedList{"pka"},
			Columns: []*model.Column{
				{Name: "pka"},
				{Name: "pkb"},
				{Name: "ukc"},
				{Name: "ifkd", ForeignTable: "other", ForeignColumn: "pka", ForeignKeyName: "test_ifkd_fk"},
				{Name: "cfke"},
			},
			Constraints: []*model.Constraint{
				{Name: "test_cfke_fk", Type: model.ConstraintTypeForeign, Definition: "(cfke) REFERENCES public.other (pka)"},
				{Name: "test_uqc_idx", Type: model.ConstraintTypeUnique, Definition: "(uqc)"},
			},
		},
		&model.Table{
			Name:       "other",
			PrimaryKey: model.DelimitedList{"pka"},
			Columns: []*model.Column{
				{Name: "pka"},
				{Name: "pkb"},
				{Name: "ukc"},
				{Name: "ifkd"},
				{Name: "cfke"},
			},
		},
	},
}

func diffConstraintsTableCommon(oldSchema, newSchema *model.Schema, ctype pgsql8.ConstraintType) []output.ToSql {
	oldDoc := &model.Definition{
		Schemas: []*model.Schema{oldSchema},
	}
	newDoc := &model.Definition{
		Schemas: []*model.Schema{newSchema},
	}
	ofs := &sqltest.RecordingOfs{
		StripComments: true,
	}
	setOldNewDocs(oldDoc, newDoc)
	diffConstraints := pgsql8.GlobalDiffConstraints
	diffConstraints.DropConstraintsTable(ofs, oldSchema, oldSchema.Tables[0], newSchema, newSchema.Tables[0], ctype)
	diffConstraints.CreateConstraintsTable(ofs, oldSchema, oldSchema.Tables[0], newSchema, newSchema.Tables[0], ctype)
	if ofs.Sql == nil {
		return []output.ToSql{}
	}
	return ofs.Sql
}
