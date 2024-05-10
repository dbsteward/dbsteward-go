package pgsql8

import (
	"testing"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/format/sql99"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/stretchr/testify/assert"
)

func TestDiffConstraints_DropCreate_SameToSame(t *testing.T) {
	ddl := diffConstraintsTableCommon(t, diffConstraintsSchemaPka, diffConstraintsSchemaPka, sql99.ConstraintTypeAll)
	assert.Empty(t, ddl)
}

func TestDiffConstraints_DropCreate_AddSome(t *testing.T) {
	ddl := diffConstraintsTableCommon(t, diffConstraintsSchemaPka, diffConstraintsSchemaPkaUqc, sql99.ConstraintTypeAll)
	assert.Equal(t, []output.ToSql{
		&sql.ConstraintCreateRaw{
			Table:          sql.TableRef{Schema: "public", Table: "test"},
			Constraint:     "test_uqc_idx",
			ConstraintType: ir.ConstraintTypeUnique,
			Definition:     "(uqc)",
		},
	}, ddl)
}

func TestDiffConstraints_DropCreate_DropSome(t *testing.T) {
	ddl := diffConstraintsTableCommon(t, diffConstraintsSchemaPkaUqc, diffConstraintsSchemaPka, sql99.ConstraintTypeAll)
	assert.Equal(t, []output.ToSql{
		&sql.ConstraintDrop{
			Table:      sql.TableRef{"public", "test"},
			Constraint: "test_uqc_idx",
		},
	}, ddl)
}

func TestDiffConstraints_DropCreate_ChangeOne(t *testing.T) {
	ddl := diffConstraintsTableCommon(t, diffConstraintsSchemaPka, diffConstraintsSchemaPkb, sql99.ConstraintTypeAll)
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
	ddl := diffConstraintsTableCommon(t, diffConstraintsSchemaPka, diffConstraintsSchemaPkbCfke, sql99.ConstraintTypeAll)
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
	ddl := diffConstraintsTableCommon(t, diffConstraintsSchemaPkaUqcIfkdCfke, diffConstraintsSchemaPkbCfke, sql99.ConstraintTypeAll)
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
			{
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
			{
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
	config := DefaultConfig
	ofs := output.NewSegmenter(defaultQuoter(config))
	differ := newDiff(NewOperations(config).(*Operations), defaultQuoter(config))
	config = setOldNewDocs(config, differ, oldDoc, newDoc)
	err := dropConstraintsTable(config, ofs, oldSchema, oldSchema.Tables[0], newSchema, nil, sql99.ConstraintTypePrimaryKey)
	if err != nil {
		t.Fatal(err)
	}
	err = createConstraintsTable(config, ofs, oldSchema, oldSchema.Tables[0], newSchema, newSchema.Tables[0], sql99.ConstraintTypePrimaryKey)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, []output.ToSql{
		&sql.ConstraintDrop{
			Table:      sql.TableRef{Schema: "public", Table: "newtable"},
			Constraint: "test_pkey",
		},
		&sql.ConstraintCreatePrimaryKey{
			Table:      sql.TableRef{Schema: "public", Table: "newtable"},
			Constraint: "newtable_pkey",
			Columns:    []string{"pkb"},
		},
	}, ofs.Body)
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
	assert.Equal(t, []output.ToSql{}, diffConstraintsTableCommon(t, auto, auto, sql99.ConstraintTypeAll))
	assert.Equal(t, []output.ToSql{}, diffConstraintsTableCommon(t, noAuto, noAuto, sql99.ConstraintTypeAll))
	assert.Equal(t, []output.ToSql{}, diffConstraintsTableCommon(t, noAuto, auto, sql99.ConstraintTypePrimaryKey))
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

	ddl := diffConstraintsTableCommon(t, oldSchema, newSchema, sql99.ConstraintTypeAll)
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

func diffConstraintsTableCommon(t *testing.T, oldSchema, newSchema *ir.Schema, ctype sql99.ConstraintType) []output.ToSql {
	oldDoc := &ir.Definition{
		Schemas: []*ir.Schema{oldSchema},
	}
	newDoc := &ir.Definition{
		Schemas: []*ir.Schema{newSchema},
	}
	config := DefaultConfig
	ofs := output.NewSegmenter(defaultQuoter(config))
	differ := newDiff(NewOperations(config).(*Operations), defaultQuoter(config))
	config = setOldNewDocs(config, differ, oldDoc, newDoc)
	err := dropConstraintsTable(config, ofs, oldSchema, oldSchema.Tables[0], newSchema, newSchema.Tables[0], ctype)
	if err != nil {
		t.Fatal(err)
	}
	err = createConstraintsTable(config, ofs, oldSchema, oldSchema.Tables[0], newSchema, newSchema.Tables[0], ctype)
	if err != nil {
		t.Fatal(err)
	}
	if ofs.Body == nil {
		return []output.ToSql{}
	}
	return ofs.Body
}
