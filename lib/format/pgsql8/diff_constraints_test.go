package pgsql8

import (
	"testing"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/format/sql99"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/pgtestutil"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/stretchr/testify/assert"
)

func TestDiffConstraints_DropCreate_SameToSame(t *testing.T) {
	ddl := diffConstraintsTableCommon(diffConstraintsSchemaPka, diffConstraintsSchemaPka, sql99.ConstraintTypeAll)
	assert.Equal(t, []output.ToSql{}, ddl)
}

func TestDiffConstraints_DropCreate_AddSome(t *testing.T) {
	ddl := diffConstraintsTableCommon(diffConstraintsSchemaPka, diffConstraintsSchemaPkaUqc, sql99.ConstraintTypeAll)
	assert.Equal(t, []output.ToSql{
		&sql.ConstraintCreateRaw{
			Table:          sql.TableRef{"public", "test"},
			Constraint:     "test_uqc_idx",
			ConstraintType: ir.ConstraintTypeUnique,
			Definition:     "(uqc)",
		},
	}, ddl)
}

func TestDiffConstraints_DropCreate_DropSome(t *testing.T) {
	ddl := diffConstraintsTableCommon(diffConstraintsSchemaPkaUqc, diffConstraintsSchemaPka, sql99.ConstraintTypeAll)
	assert.Equal(t, []output.ToSql{
		&sql.ConstraintDrop{
			Table:      sql.TableRef{"public", "test"},
			Constraint: "test_uqc_idx",
		},
	}, ddl)
}

func TestDiffConstraints_DropCreate_ChangeOne(t *testing.T) {
	ddl := diffConstraintsTableCommon(diffConstraintsSchemaPka, diffConstraintsSchemaPkb, sql99.ConstraintTypeAll)
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
	ddl := diffConstraintsTableCommon(diffConstraintsSchemaPka, diffConstraintsSchemaPkbCfke, sql99.ConstraintTypeAll)
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
			ConstraintType: ir.ConstraintTypeForeign,
			Definition:     "(cfke) REFERENCES public.other (pka)",
		},
	}, ddl)
}

func TestDiffConstraints_DropCreate_DropSomeAndChange(t *testing.T) {
	ddl := diffConstraintsTableCommon(diffConstraintsSchemaPkaUqcIfkdCfke, diffConstraintsSchemaPkbCfke, sql99.ConstraintTypeAll)
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
	oldSchema := &ir.Schema{
		Name: "public",
		Tables: []*ir.Table{
			&ir.Table{
				Name:       "test",
				PrimaryKey: []string{"pka"},
				Columns: []*ir.Column{
					{Name: "pka", Type: "int"},
				},
			},
		},
	}
	newSchema := &ir.Schema{
		Name: "public",
		Tables: []*ir.Table{
			&ir.Table{
				Name:          "newtable",
				PrimaryKey:    []string{"pkb"},
				OldSchemaName: "public",
				OldTableName:  "test",
				Columns: []*ir.Column{
					{Name: "pkb", Type: "int", OldColumnName: "pka"},
				},
			},
		},
	}

	// in psql8_diff::update_structure() when the new schema doesn't contain the old table name,
	// $new_table is set to null for the first diff_constraints_table() call, adjusted this test accordingly
	// ddl := diffConstraintsTableCommon(oldSchema, newSchema, sql99.ConstraintTypePrimaryKey)
	oldDoc := &ir.Definition{
		Schemas: []*ir.Schema{oldSchema},
	}
	newDoc := &ir.Definition{
		Schemas: []*ir.Schema{newSchema},
	}
	ofs := &pgtestutil.RecordingOfs{
		StripComments: true,
	}
	setOldNewDocs(oldDoc, newDoc)
	dropConstraintsTable(ofs, oldSchema, oldSchema.Tables[0], newSchema, nil, sql99.ConstraintTypePrimaryKey)
	createConstraintsTable(ofs, oldSchema, oldSchema.Tables[0], newSchema, newSchema.Tables[0], sql99.ConstraintTypePrimaryKey)

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

// TODO(go,pgsql) This test exists as tests/pgsql8/Pgsql8ConstraintDiffPgtestutil.php testAutoIncrement() but I can't find
// any reference to dealing with `auto_increment` type flags in postgres, everything is gated behind sqlformat==mysql5
// I don't know why this works in v1, but we should figure it out

func TestDiffConstraints_DropCreate_AutoIncrement(t *testing.T) {
	t.SkipNow()

	auto := &ir.Schema{
		Name: "public",
		Tables: []*ir.Table{
			&ir.Table{
				Name:       "test",
				PrimaryKey: []string{"pka"},
				Columns: []*ir.Column{
					{Name: "pka", Type: "int auto_increment"},
				},
			},
		},
	}
	noAuto := &ir.Schema{
		Name: "public",
		Tables: []*ir.Table{
			&ir.Table{
				Name:       "newtable",
				PrimaryKey: []string{"pka"},
				Columns: []*ir.Column{
					{Name: "pka", Type: "int"},
				},
			},
		},
	}

	// auto-increment is no longer considered a constraint, but rather part of a type, and is calculated during the tables diff
	assert.Equal(t, []output.ToSql{}, diffConstraintsTableCommon(auto, auto, sql99.ConstraintTypeAll))
	assert.Equal(t, []output.ToSql{}, diffConstraintsTableCommon(noAuto, noAuto, sql99.ConstraintTypeAll))
	assert.Equal(t, []output.ToSql{}, diffConstraintsTableCommon(noAuto, auto, sql99.ConstraintTypePrimaryKey))
}

func TestDiffConstraints_DropCreate_ChangeColumnTypeWithFK(t *testing.T) {
	oldSchema := &ir.Schema{
		Name: "public",
		Tables: []*ir.Table{
			&ir.Table{
				Name:       "test",
				PrimaryKey: []string{"pka"},
				Columns: []*ir.Column{
					{Name: "pka", Type: "int"},
					{Name: "ifkd", ForeignTable: "other", ForeignColumn: "pka", ForeignKeyName: "test_ifkd_fk"},
				},
			},
			&ir.Table{
				Name:       "other",
				PrimaryKey: []string{"pka"},
				Columns: []*ir.Column{
					{Name: "pka", Type: "int"},
				},
			},
		},
	}
	// changed type of other.pka from int to text
	newSchema := &ir.Schema{
		Name: "public",
		Tables: []*ir.Table{
			&ir.Table{
				Name:       "test",
				PrimaryKey: []string{"pka"},
				Columns: []*ir.Column{
					{Name: "pka", Type: "int"},
					{Name: "ifkd", ForeignTable: "other", ForeignColumn: "pka", ForeignKeyName: "test_ifkd_fk"},
				},
			},
			&ir.Table{
				Name:       "other",
				PrimaryKey: []string{"pka"},
				Columns: []*ir.Column{
					{Name: "pka", Type: "text"},
				},
			},
		},
	}

	ddl := diffConstraintsTableCommon(oldSchema, newSchema, sql99.ConstraintTypeAll)
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

var diffConstraintsSchemaPka = &ir.Schema{
	Name: "public",
	Tables: []*ir.Table{
		&ir.Table{
			Name:       "test",
			PrimaryKey: []string{"pka"},
			Columns: []*ir.Column{
				{Name: "pka"},
				{Name: "pkb"},
				{Name: "ukc"},
				{Name: "ifkd"},
				{Name: "cfke"},
			},
		},
	},
}

var diffConstraintsSchemaPkb = &ir.Schema{
	Name: "public",
	Tables: []*ir.Table{
		&ir.Table{
			Name:       "test",
			PrimaryKey: []string{"pkb"},
			Columns: []*ir.Column{
				{Name: "pka"},
				{Name: "pkb"},
				{Name: "ukc"},
				{Name: "ifkd"},
				{Name: "cfke"},
			},
		},
	},
}

var diffConstraintsSchemaPkaUqc = &ir.Schema{
	Name: "public",
	Tables: []*ir.Table{
		&ir.Table{
			Name:       "test",
			PrimaryKey: []string{"pka"},
			Columns: []*ir.Column{
				{Name: "pka"},
				{Name: "pkb"},
				{Name: "ukc"},
				{Name: "ifkd"},
				{Name: "cfke"},
			},
			Constraints: []*ir.Constraint{
				{Name: "test_uqc_idx", Type: ir.ConstraintTypeUnique, Definition: "(uqc)"},
			},
		},
	},
}

var diffConstraintsSchemaPkbCfke = &ir.Schema{
	Name: "public",
	Tables: []*ir.Table{
		&ir.Table{
			Name:       "test",
			PrimaryKey: []string{"pkb"},
			Columns: []*ir.Column{
				{Name: "pka"},
				{Name: "pkb"},
				{Name: "ukc"},
				{Name: "ifkd"},
				{Name: "cfke"},
			},
			Constraints: []*ir.Constraint{
				{Name: "test_cfke_fk", Type: ir.ConstraintTypeForeign, Definition: "(cfke) REFERENCES public.other (pka)"},
			},
		},
		&ir.Table{
			Name:       "other",
			PrimaryKey: []string{"pka"},
			Columns: []*ir.Column{
				{Name: "pka"},
				{Name: "pkb"},
				{Name: "ukc"},
				{Name: "ifkd"},
				{Name: "cfke"},
			},
		},
	},
}

var diffConstraintsSchemaPkaUqcIfkdCfke = &ir.Schema{
	Name: "public",
	Tables: []*ir.Table{
		&ir.Table{
			Name:       "test",
			PrimaryKey: []string{"pka"},
			Columns: []*ir.Column{
				{Name: "pka"},
				{Name: "pkb"},
				{Name: "ukc"},
				{Name: "ifkd", ForeignTable: "other", ForeignColumn: "pka", ForeignKeyName: "test_ifkd_fk"},
				{Name: "cfke"},
			},
			Constraints: []*ir.Constraint{
				{Name: "test_cfke_fk", Type: ir.ConstraintTypeForeign, Definition: "(cfke) REFERENCES public.other (pka)"},
				{Name: "test_uqc_idx", Type: ir.ConstraintTypeUnique, Definition: "(uqc)"},
			},
		},
		&ir.Table{
			Name:       "other",
			PrimaryKey: []string{"pka"},
			Columns: []*ir.Column{
				{Name: "pka"},
				{Name: "pkb"},
				{Name: "ukc"},
				{Name: "ifkd"},
				{Name: "cfke"},
			},
		},
	},
}

func diffConstraintsTableCommon(oldSchema, newSchema *ir.Schema, ctype sql99.ConstraintType) []output.ToSql {
	oldDoc := &ir.Definition{
		Schemas: []*ir.Schema{oldSchema},
	}
	newDoc := &ir.Definition{
		Schemas: []*ir.Schema{newSchema},
	}
	ofs := &pgtestutil.RecordingOfs{
		StripComments: true,
	}
	setOldNewDocs(oldDoc, newDoc)
	dropConstraintsTable(ofs, oldSchema, oldSchema.Tables[0], newSchema, newSchema.Tables[0], ctype)
	createConstraintsTable(ofs, oldSchema, oldSchema.Tables[0], newSchema, newSchema.Tables[0], ctype)
	if ofs.Sql == nil {
		return []output.ToSql{}
	}
	return ofs.Sql
}
