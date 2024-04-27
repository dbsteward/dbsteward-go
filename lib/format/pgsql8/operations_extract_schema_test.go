package pgsql8

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/stretchr/testify/assert"
)

var PG_8_0 VersionNum = NewVersionNum(8, 0)

// TODO(go,3) is there a way to make this set of tests a whole lot less annoying?
// TODO(go,pgsql) the v1 ExtractionTest tested what is now ExtractSchema, Introspector, Connection, _and_ postgres
//                but this only tests ExtractSchema. We still should test the other layers, and come up with a story
//                around e2e testing with a real db connection.

func TestOperations_ExtractSchema_Indexes(t *testing.T) {
	pgDoc := structure{
		Version: PG_8_0,
		Schemas: []schemaEntry{{
			Name: "public",
		}},
		Tables: []tableEntry{{
			Schema: "public",
			Table:  "test",
			Columns: []columnEntry{
				{
					Name:     "col1",
					AttrType: "text",
					Position: 1,
				},
				{
					Name:     "col2",
					AttrType: "text",
					Position: 2,
				},
				{
					Name:     "col3",
					AttrType: "text",
					Position: 3,
				},
			},
			Indexes: []indexEntry{
				// test that both column and functional expressions work as expected
				{
					Name: "testidx",
					Dimensions: []string{
						"lower(col1)",
						"col2",
						"(col1 || ';;'::text)",
						"col3",
						"\"overlay\"(btrim(col2), 'x'::text, 2)",
					},
				},
				// test that index column order is extracted correctly
				{
					Name:       "testidx2",
					Dimensions: []string{"col1", "col2", "col3"},
				},
				{
					Name:       "testidx3",
					Dimensions: []string{"col2", "col1", "col3"},
				},
			},
		}},
	}
	// TODO(feat) test changing Using
	// TODO(feat) test conditional index
	// TODO(feat) test unique index
	// TODO(feat) assert that .Sql = true
	expected := []*ir.Index{
		{
			Name:  "testidx",
			Using: "btree",
			Dimensions: []*ir.IndexDim{
				{Name: "testidx_1", Value: "lower(col1)"},
				{Name: "testidx_2", Value: "col2"},
				{Name: "testidx_3", Value: "(col1 || ';;'::text)"},
				{Name: "testidx_4", Value: "col3"},
				{Name: "testidx_5", Value: "\"overlay\"(btrim(col2), 'x'::text, 2)"},
			},
		},
		{
			Name:  "testidx2",
			Using: "btree",
			Dimensions: []*ir.IndexDim{
				{Name: "testidx2_1", Value: "col1"},
				{Name: "testidx2_2", Value: "col2"},
				{Name: "testidx2_3", Value: "col3"},
			},
		},
		{
			Name:  "testidx3",
			Using: "btree",
			Dimensions: []*ir.IndexDim{
				{Name: "testidx3_1", Value: "col2"},
				{Name: "testidx3_2", Value: "col1"},
				{Name: "testidx3_3", Value: "col3"},
			},
		},
	}
	ops := NewOperations()
	actual, err := ops.pgToIR(pgDoc)
	if err != nil {
		t.Fatalf("Conversion failed: %+v", err)
	}
	assert.Equal(t, expected, actual.Schemas[0].Tables[0].Indexes)
}

func TestOperations_ExtractSchema_CompoundUniqueConstraint(t *testing.T) {
	pgDoc := structure{
		Version: PG_8_0,
		Schemas: []schemaEntry{{
			Name: "public",
		}},
		Tables: []tableEntry{
			{
				Schema: "public",
				Table:  "test",
				Columns: []columnEntry{
					{
						Name:     "col1",
						AttrType: "bigint",
						Nullable: false,
						Position: 1,
					},
					{
						Name:     "col2",
						AttrType: "bigint",
						Nullable: false,
						Position: 2,
					},
					{
						Name:     "col3",
						AttrType: "character varying(20)",
						Nullable: false,
						Position: 3,
					},
					{
						Name:     "col4",
						AttrType: "character varying(20)",
						Nullable: true,
						Position: 3,
					},
				},
			},
		},
		Constraints: []constraintEntry{
			{
				Schema:  "public",
				Table:   "test",
				Name:    "test_constraint",
				Type:    "u",
				Columns: []string{"col2", "col3", "col4"},
			},
		},
	}
	ops := NewOperations()
	actual, err := ops.pgToIR(pgDoc)
	if err != nil {
		t.Fatalf("Conversion failed: %+v", err)
	}

	// compound constraints should not set individual column uniqueness
	assert.False(t, actual.Schemas[0].Tables[0].Columns[1].Unique)
	assert.False(t, actual.Schemas[0].Tables[0].Columns[2].Unique)
	assert.False(t, actual.Schemas[0].Tables[0].Columns[3].Unique)

	// TODO(go,pgsql) why is this quoted, it shouldn't be quoted unless we turn it on.... right?
	assert.Equal(t, []*ir.Constraint{
		{Name: "test_constraint", Type: ir.ConstraintTypeUnique, Definition: `("col2", "col3", "col4")`},
	}, actual.Schemas[0].Tables[0].Constraints)
}

func TestOperations_ExtractSchema_TableComments(t *testing.T) {
	const (
		schemaDesc = "A description of the public schema"
		tableDesc  = "A description of the test table"
		colDesc    = "A description of col1 on the test table"
	)
	pgDoc := structure{
		Version: PG_8_0,
		Schemas: []schemaEntry{{
			Name:        "public",
			Description: schemaDesc,
		}},
		Tables: []tableEntry{
			{
				Schema:            "public",
				SchemaDescription: schemaDesc,
				Table:             "test",
				TableDescription:  tableDesc,
				Columns: []columnEntry{
					{
						Name:        "col1",
						AttrType:    "text",
						Description: colDesc,
					},
				},
			},
		},
		Constraints: []constraintEntry{
			{
				Schema:  "public",
				Table:   "test",
				Name:    "test_col1_pkey",
				Type:    "p",
				Columns: []string{"col1"},
			},
		},
	}
	ops := NewOperations()
	actual, err := ops.pgToIR(pgDoc)
	if err != nil {
		t.Fatalf("Conversion failed: %+v", err)
	}
	assert.Equal(t, schemaDesc, actual.Schemas[0].Description)
	assert.Equal(t, tableDesc, actual.Schemas[0].Tables[0].Description)
	assert.Equal(t, colDesc, actual.Schemas[0].Tables[0].Columns[0].Description)
}

func TestOperations_ExtractSchema_FunctionAmpersands(t *testing.T) {
	body := strings.TrimSpace(`
DECLARE
	overlap boolean;
BEGIN
	overlap := $1 && $2;
	RETURN overlap;
END;
`)
	pgDoc := structure{
		Version: PG_8_0,
		Schemas: []schemaEntry{{
			Name: "public",
		}},
		Functions: []functionEntry{
			{
				Oid:        Oid{1},
				Schema:     "public",
				Name:       "rates_overlap",
				Return:     "boolean",
				Type:       "normal",
				Volatility: "VOLATILE",
				Owner:      "app",
				Language:   "plpgsql",
				Source:     body,
				Args: []functionArgEntry{
					{"rates_a", "money", "IN"},
					{"rates_b", "money", "IN"},
				},
			},
		},
	}
	ops := NewOperations()
	actual, err := ops.pgToIR(pgDoc)
	if err != nil {
		t.Fatalf("Conversion failed: %+v", err)
	}
	assert.Equal(t, []*ir.Function{
		{
			Name:        "rates_overlap",
			Owner:       "app",
			Returns:     "boolean",
			CachePolicy: "VOLATILE",
			Parameters: []*ir.FunctionParameter{
				{Name: "rates_a", Type: "money", Direction: "IN"},
				{Name: "rates_b", Type: "money", Direction: "IN"},
			},
			Definitions: []*ir.FunctionDefinition{
				{SqlFormat: ir.SqlFormatPgsql8, Language: "plpgsql", Text: body},
			},
		},
	}, actual.Schemas[0].Functions)
}

func TestOperations_ExtractSchema_FunctionArgs(t *testing.T) {
	const body = `BEGIN RETURN 1; END;`
	pgDoc := structure{
		Version: PG_8_0,
		Schemas: []schemaEntry{{
			Name: "public",
		}},
		Functions: []functionEntry{
			{ // array type and argument names
				Oid:      Oid{1},
				Schema:   "public",
				Name:     "increment1",
				Return:   "integer",
				Type:     "normal",
				Language: "plpgsql",
				Source:   body,
				Args: []functionArgEntry{
					{"arg1", "integer[]", "IN"},
					{"arg2", "uuid[]", "IN"},
				},
			},
			{ // array type and no argument names
				Oid:      Oid{2},
				Schema:   "public",
				Name:     "increment2",
				Return:   "integer",
				Type:     "normal",
				Language: "plpgsql",
				Source:   body,
				Args: []functionArgEntry{
					{"", "integer[]", "IN"},
					{"", "uuid[]", "IN"},
				},
			},
			{ // array type and mixed argument names
				Oid:      Oid{3},
				Schema:   "public",
				Name:     "increment3",
				Return:   "integer",
				Type:     "normal",
				Language: "plpgsql",
				Source:   body,
				Args: []functionArgEntry{
					{"arg1", "integer[]", "IN"},
					{"", "uuid[]", "IN"},
				},
			},
			{
				Oid:      Oid{4},
				Schema:   "public",
				Name:     "increment4",
				Return:   "integer",
				Type:     "normal",
				Language: "plpgsql",
				Source:   body,
				Args: []functionArgEntry{
					{"", "integer[]", "IN"},
					{"arg1", "uuid[]", "IN"},
				},
			},
		},
	}
	ops := NewOperations()
	actual, err := ops.pgToIR(pgDoc)
	if err != nil {
		t.Fatalf("Conversion failed: %+v", err)
	}
	schema := actual.Schemas[0]
	assert.Equal(t, "arg1", schema.Functions[0].Parameters[0].Name)
	assert.Equal(t, "integer[]", schema.Functions[0].Parameters[0].Type)
	assert.Equal(t, "arg2", schema.Functions[0].Parameters[1].Name)
	assert.Equal(t, "uuid[]", schema.Functions[0].Parameters[1].Type)

	assert.Equal(t, "", schema.Functions[1].Parameters[0].Name)
	assert.Equal(t, "integer[]", schema.Functions[1].Parameters[0].Type)
	assert.Equal(t, "", schema.Functions[1].Parameters[1].Name)
	assert.Equal(t, "uuid[]", schema.Functions[1].Parameters[1].Type)

	assert.Equal(t, "arg1", schema.Functions[2].Parameters[0].Name)
	assert.Equal(t, "integer[]", schema.Functions[2].Parameters[0].Type)
	assert.Equal(t, "", schema.Functions[2].Parameters[1].Name)
	assert.Equal(t, "uuid[]", schema.Functions[2].Parameters[1].Type)

	assert.Equal(t, "", schema.Functions[3].Parameters[0].Name)
	assert.Equal(t, "integer[]", schema.Functions[3].Parameters[0].Type)
	assert.Equal(t, "arg1", schema.Functions[3].Parameters[1].Name)
	assert.Equal(t, "uuid[]", schema.Functions[3].Parameters[1].Type)
}

func TestOperations_ExtractSchema_TableArrayType(t *testing.T) {
	pgDoc := structure{
		Version: PG_8_0,
		Schemas: []schemaEntry{{
			Name: "public",
		}},
		Tables: []tableEntry{
			{
				Schema: "public",
				Table:  "test",
				Columns: []columnEntry{
					{Name: "name", AttrType: "text[]"},
				},
			},
		},
	}
	ops := NewOperations()
	actual, err := ops.pgToIR(pgDoc)
	if err != nil {
		t.Fatalf("Conversion failed: %+v", err)
	}
	assert.Equal(t, "text[]", actual.Schemas[0].Tables[0].Columns[0].Type)
}

func TestOperations_ExtractSchema_FKReferentialConstraints(t *testing.T) {
	// CREATE TABLE dummy (foo int, bar varchar(32), PRIMARY KEY (foo, bar));
	// CREATE TABLE test (
	// 	id int PRIMARY KEY,
	// 	foo int,
	// 	bar varchar(32),
	// 	FOREIGN KEY (foo, bar) REFERENCES dummy (foo, bar)
	// 		ON UPDATE NO ACTION
	// 		ON DELETE SET NULL
	// );
	pgDoc := structure{
		Version: PG_8_0,
		Schemas: []schemaEntry{{
			Name: "public",
		}},
		Tables: []tableEntry{
			{
				Schema: "public",
				Table:  "dummy",
				Columns: []columnEntry{
					{Name: "feux", AttrType: "integer"},
					{Name: "barf", AttrType: "character varying(32)"},
				},
			},
			{
				Schema: "public",
				Table:  "test",
				Columns: []columnEntry{
					{Name: "id", AttrType: "integer"},
					{Name: "foo", AttrType: "integer"},
					{Name: "bar", AttrType: "character varying(32)"},
				},
			},
		},
		Constraints: []constraintEntry{
			{Schema: "public", Table: "dummy", Name: "dummy_pkey", Type: "p", Columns: []string{"foo", "bar"}},
			{Schema: "public", Table: "test", Name: "test_pkey", Type: "p", Columns: []string{"id"}},
		},
		ForeignKeys: []foreignKeyEntry{
			{
				ConstraintName: "test_foo_fkey",
				UpdateRule:     "a",
				DeleteRule:     "n",
				LocalSchema:    "public",
				LocalTable:     "test",
				LocalColumns:   []string{"foo", "bar"},
				ForeignSchema:  "public",
				ForeignTable:   "dummy",
				ForeignColumns: []string{"feux", "barf"},
			},
		},
	}
	ops := NewOperations()
	actual, err := ops.pgToIR(pgDoc)
	if err != nil {
		t.Fatalf("Conversion failed: %+v", err)
	}
	assert.Equal(t, []*ir.ForeignKey{
		{
			ConstraintName: "test_foo_fkey",
			Columns:        []string{"foo", "bar"},
			ForeignSchema:  "public",
			ForeignTable:   "dummy",
			ForeignColumns: []string{"feux", "barf"},
			OnUpdate:       ir.ForeignKeyActionNoAction,
			OnDelete:       ir.ForeignKeyActionSetNull,
		},
	}, actual.Schemas[0].Tables[1].ForeignKeys)
	// these should _not_ be omitted in this case, because it's a separate element
	assert.Equal(t, "integer", actual.Schemas[0].Tables[1].Columns[1].Type)
	assert.Equal(t, "character varying(32)", actual.Schemas[0].Tables[1].Columns[2].Type)
}

func TestOperations_ExtractSchema_Sequences(t *testing.T) {
	// Note: this one test covers the v1 tests:
	// - IsolatedSequenceTest::testPublicSequencesBuildProperly (a)
	// - IsolatedSequenceTest::testIsolatedSequencesBuildProperly (a)
	// - IsolatedSequenceTest::testIntSequencesBecomeSerials (b)
	// - IsolatedSequenceTest::testNoTableSequencesBuilds (a)
	// - ExtractionTest::testDoNotExtractSequenceFromTable (b)
	// the ones marked (a) test whether a seq which is not used as a default in a column is extracted as a sequence element
	// the ones marked (b) test that sequences which are used as defaults in a column are extracted as a serial column instead
	pgDoc := structure{
		Version: PG_8_0,
		Schemas: []schemaEntry{{
			Name: "public",
		}},
		Tables: []tableEntry{
			{
				Schema: "public",
				Table:  "user",
				Columns: []columnEntry{
					{Name: "user_id", AttrType: "integer", Default: "nextval('test_seq')"},
					{Name: "user_name", AttrType: "varchar(100)"},
				},
			},
		},
		Sequences: []sequenceRelEntry{
			{
				Schema: "public",
				Name:   "blah",
				Owner:  "owner",
				Cache:  sql.NullInt64{Int64: 5, Valid: true},
				Min:    sql.NullInt64{Int64: 3, Valid: true},
				Max:    sql.NullInt64{Int64: 10, Valid: true},
			},
		},
		Constraints: []constraintEntry{
			{Schema: "public", Table: "user", Name: "user_pkey", Type: "p", Columns: []string{"user_id"}},
		},
	}
	ops := NewOperations()
	actual, err := ops.pgToIR(pgDoc)
	if err != nil {
		t.Fatalf("Conversion failed: %+v", err)
	}
	// Test that int sequences become serials
	// TODO(go,3) this doesn't feel right - does an int/nextval column have different semantics than a serial type?
	//            It feels wrong that we simply don't extract the sequence. I'd rather extract it as-is and let the
	//            user sort it all out. Will maintain v1 behavior for now though.

	assert.Equal(t, "serial", actual.Schemas[0].Tables[0].Columns[0].Type)
	assert.Equal(t, []*ir.Sequence{
		// test_seq SHOULD NOT be extracted (see `TestOperations_ExtractSchema_DoNotExtractSequenceFromSerial`)
		// blah SHOULD be extracted because it's not linked to
		{
			Name:  "blah",
			Owner: "owner",
			Cache: util.Some(5),
			Min:   util.Some(3),
			Max:   util.Some(10),
		},
	}, actual.Schemas[0].Sequences)
}
