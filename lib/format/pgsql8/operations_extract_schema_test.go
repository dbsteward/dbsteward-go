package pgsql8_test

import (
	"strings"
	"testing"

	"github.com/dbsteward/dbsteward/lib/util"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/live"
	"github.com/dbsteward/dbsteward/lib/model"
)

var PG_8_0 live.VersionNum = live.NewVersionNum(8, 0)

// TODO(go,3) is there a way to make this set of tests a whole lot less annoying?
// TODO(go,pgsql) the v1 ExtractionTest tested what is now ExtractSchema, Introspector, Connection, _and_ postgres
//                but this only tests ExtractSchema. We still should test the other layers, and come up with a story
//                around e2e testing with a real db connection.

/* Copy+paste and change for each test:
func TestOperations_ExtractSchema_Something(t *testing.T) {
	ctrl := gomock.NewController(t)
	introspector := live.NewMockIntrospector(ctrl)

	introspector.EXPECT().GetSchemaOwner(gomock.Any()).AnyTimes()
	introspector.EXPECT().GetTableList().AnyTimes()
	introspector.EXPECT().GetColumns(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetTableStorageOptions(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetSequenceRelList(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetIndexes(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetConstraints().AnyTimes()
	introspector.EXPECT().GetForeignKeys().AnyTimes()
	introspector.EXPECT().GetFunctions().AnyTimes()
	introspector.EXPECT().GetFunctionArgs(gomock.Any()).AnyTimes()
	introspector.EXPECT().GetTriggers().AnyTimes()
	introspector.EXPECT().GetViews().AnyTimes()
	introspector.EXPECT().GetTablePerms().AnyTimes()
	introspector.EXPECT().GetSequencePerms(gomock.Any()).AnyTimes()

	schema := commonExtract(introspector)
	assert.Equal(t, ...)
}
*/

func TestOperations_ExtractSchema_Indexes(t *testing.T) {
	ctrl := gomock.NewController(t)
	introspector := live.NewMockIntrospector(ctrl)

	introspector.EXPECT().GetSchemaOwner(gomock.Any()).AnyTimes()
	introspector.EXPECT().GetTableStorageOptions(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetSequenceRelList(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetViews().AnyTimes()
	introspector.EXPECT().GetConstraints().AnyTimes()
	introspector.EXPECT().GetForeignKeys().AnyTimes()
	introspector.EXPECT().GetFunctions().AnyTimes()
	introspector.EXPECT().GetTriggers().AnyTimes()
	introspector.EXPECT().GetTablePerms().AnyTimes()
	introspector.EXPECT().GetSequencePerms(gomock.Any()).AnyTimes()

	introspector.EXPECT().GetTableList().Return([]live.TableEntry{
		live.TableEntry{
			Schema: "public",
			Table:  "test",
		},
	}, nil)
	introspector.EXPECT().GetColumns("public", "test").Return([]live.ColumnEntry{
		live.ColumnEntry{
			Name:     "col1",
			AttrType: "text",
			Position: 1,
		},
		live.ColumnEntry{
			Name:     "col2",
			AttrType: "text",
			Position: 2,
		},
		live.ColumnEntry{
			Name:     "col3",
			AttrType: "text",
			Position: 3,
		},
	}, nil)

	introspector.EXPECT().GetIndexes("public", "test").Return([]live.IndexEntry{
		// test that both column and functional expressions work as expected
		live.IndexEntry{
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
		live.IndexEntry{
			Name:       "testidx2",
			Dimensions: []string{"col1", "col2", "col3"},
		},
		live.IndexEntry{
			Name:       "testidx3",
			Dimensions: []string{"col2", "col1", "col3"},
		},
	}, nil)

	schema := commonExtract(introspector, PG_8_0)

	// TODO(feat) test changing Using
	// TODO(feat) test conditional index
	// TODO(feat) test unique index
	// TODO(feat) assert that .Sql = true
	expected := []*model.Index{
		&model.Index{
			Name:  "testidx",
			Using: "btree",
			Dimensions: []*model.IndexDim{
				{Name: "testidx_1", Value: "lower(col1)"},
				{Name: "testidx_2", Value: "col2"},
				{Name: "testidx_3", Value: "(col1 || ';;'::text)"},
				{Name: "testidx_4", Value: "col3"},
				{Name: "testidx_5", Value: "\"overlay\"(btrim(col2), 'x'::text, 2)"},
			},
		},
		&model.Index{
			Name:  "testidx2",
			Using: "btree",
			Dimensions: []*model.IndexDim{
				{Name: "testidx2_1", Value: "col1"},
				{Name: "testidx2_2", Value: "col2"},
				{Name: "testidx2_3", Value: "col3"},
			},
		},
		&model.Index{
			Name:  "testidx3",
			Using: "btree",
			Dimensions: []*model.IndexDim{
				{Name: "testidx3_1", Value: "col2"},
				{Name: "testidx3_2", Value: "col1"},
				{Name: "testidx3_3", Value: "col3"},
			},
		},
	}
	// test the full slice of indexes to ensure we don't do something weird like duplicate/split the index
	assert.Equal(t, expected, schema.Tables[0].Indexes)
}

func TestOperations_ExtractSchema_CompoundUniqueConstraint(t *testing.T) {
	ctrl := gomock.NewController(t)
	introspector := live.NewMockIntrospector(ctrl)

	introspector.EXPECT().GetSchemaOwner(gomock.Any()).AnyTimes()
	introspector.EXPECT().GetTableStorageOptions(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetSequenceRelList(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetViews().AnyTimes()
	introspector.EXPECT().GetForeignKeys().AnyTimes()
	introspector.EXPECT().GetFunctions().AnyTimes()
	introspector.EXPECT().GetTriggers().AnyTimes()
	introspector.EXPECT().GetTablePerms().AnyTimes()
	introspector.EXPECT().GetSequencePerms(gomock.Any()).AnyTimes()
	introspector.EXPECT().GetIndexes("public", "test").AnyTimes()

	introspector.EXPECT().GetTableList().Return([]live.TableEntry{
		live.TableEntry{
			Schema: "public",
			Table:  "test",
		},
	}, nil)
	introspector.EXPECT().GetColumns("public", "test").Return([]live.ColumnEntry{
		live.ColumnEntry{
			Name:     "col1",
			AttrType: "bigint",
			Nullable: false,
			Position: 1,
		},
		live.ColumnEntry{
			Name:     "col2",
			AttrType: "bigint",
			Nullable: false,
			Position: 2,
		},
		live.ColumnEntry{
			Name:     "col3",
			AttrType: "character varying(20)",
			Nullable: false,
			Position: 3,
		},
		live.ColumnEntry{
			Name:     "col4",
			AttrType: "character varying(20)",
			Nullable: true,
			Position: 3,
		},
	}, nil)

	introspector.EXPECT().GetConstraints().Return([]live.ConstraintEntry{
		live.ConstraintEntry{
			Schema:  "public",
			Table:   "test",
			Name:    "test_constraint",
			Type:    "u",
			Columns: []string{"col2", "col3", "col4"},
		},
	}, nil)

	schema := commonExtract(introspector, PG_8_0)

	// compound constraints should not set individual column uniqueness
	assert.False(t, schema.Tables[0].Columns[1].Unique)
	assert.False(t, schema.Tables[0].Columns[2].Unique)
	assert.False(t, schema.Tables[0].Columns[3].Unique)

	// TODO(go,pgsql) why is this quoted, it shouldn't be quoted unless we turn it on.... right?
	assert.Equal(t, []*model.Constraint{
		{Name: "test_constraint", Type: model.ConstraintTypeUnique, Definition: `("col2", "col3", "col4")`},
	}, schema.Tables[0].Constraints)
}

func TestOperations_ExtractSchema_TableComments(t *testing.T) {
	ctrl := gomock.NewController(t)
	introspector := live.NewMockIntrospector(ctrl)

	schemaDesc := "A description of the public schema"
	tableDesc := "A description of the test table"
	colDesc := "A description of col1 on the test table"

	introspector.EXPECT().GetSchemaOwner(gomock.Any()).AnyTimes()
	introspector.EXPECT().GetTableList().Return([]live.TableEntry{
		live.TableEntry{
			Schema:            "public",
			SchemaDescription: schemaDesc,
			Table:             "test",
			TableDescription:  tableDesc,
		},
	}, nil)
	introspector.EXPECT().GetColumns(gomock.Any(), gomock.Any()).Return([]live.ColumnEntry{
		live.ColumnEntry{
			Name:        "col1",
			AttrType:    "text",
			Description: colDesc,
		},
	}, nil)
	introspector.EXPECT().GetTableStorageOptions(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetSequenceRelList(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetIndexes(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetConstraints().Return([]live.ConstraintEntry{
		{Schema: "public", Table: "test", Name: "test_col1_pkey", Type: "p", Columns: []string{"col1"}},
	}, nil)
	introspector.EXPECT().GetForeignKeys().AnyTimes()
	introspector.EXPECT().GetFunctions().AnyTimes()
	introspector.EXPECT().GetTriggers().AnyTimes()
	introspector.EXPECT().GetViews().AnyTimes()
	introspector.EXPECT().GetTablePerms().AnyTimes()
	introspector.EXPECT().GetSequencePerms(gomock.Any()).AnyTimes()

	schema := commonExtract(introspector, PG_8_0)

	assert.Equal(t, schemaDesc, schema.Description)
	assert.Equal(t, tableDesc, schema.Tables[0].Description)
	assert.Equal(t, colDesc, schema.Tables[0].Columns[0].Description)
}

func TestOperations_ExtractSchema_FunctionAmpersands(t *testing.T) {
	ctrl := gomock.NewController(t)
	introspector := live.NewMockIntrospector(ctrl)

	body := strings.TrimSpace(`
DECLARE
	overlap boolean;
BEGIN
	overlap := $1 && $2;
	RETURN overlap;
END;
`)

	introspector.EXPECT().GetSchemaOwner(gomock.Any()).AnyTimes()
	introspector.EXPECT().GetTableList().AnyTimes()
	introspector.EXPECT().GetColumns(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetTableStorageOptions(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetSequenceRelList(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetIndexes(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetConstraints().AnyTimes()
	introspector.EXPECT().GetForeignKeys().AnyTimes()
	introspector.EXPECT().GetFunctions().Return([]live.FunctionEntry{
		live.FunctionEntry{
			Oid:        live.Oid{1},
			Schema:     "public",
			Name:       "rates_overlap",
			Return:     "boolean",
			Type:       "normal",
			Volatility: "VOLATILE",
			Owner:      "app",
			Language:   "plpgsql",
			Source:     body,
		},
	}, nil)
	introspector.EXPECT().GetFunctionArgs(live.Oid{1}).Return([]live.FunctionArgEntry{
		{"rates_a", "money"},
		{"rates_b", "money"},
	}, nil)
	introspector.EXPECT().GetTriggers().AnyTimes()
	introspector.EXPECT().GetViews().AnyTimes()
	introspector.EXPECT().GetTablePerms().AnyTimes()
	introspector.EXPECT().GetSequencePerms(gomock.Any()).AnyTimes()

	schema := commonExtract(introspector, PG_8_0)
	assert.Equal(t, []*model.Function{
		&model.Function{
			Name:        "rates_overlap",
			Owner:       "app",
			Returns:     "boolean",
			CachePolicy: "VOLATILE",
			Parameters: []*model.FunctionParameter{
				{Name: "rates_a", Type: "money"},
				{Name: "rates_b", Type: "money"},
			},
			Definitions: []*model.FunctionDefinition{
				{SqlFormat: model.SqlFormatPgsql8, Language: "plpgsql", Text: body},
			},
		},
	}, schema.Functions)
}

func TestOperations_ExtractSchema_FunctionArgs(t *testing.T) {
	ctrl := gomock.NewController(t)
	introspector := live.NewMockIntrospector(ctrl)

	body := `BEGIN RETURN 1; END;`

	introspector.EXPECT().GetSchemaOwner(gomock.Any()).AnyTimes()
	introspector.EXPECT().GetTableList().AnyTimes()
	introspector.EXPECT().GetColumns(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetTableStorageOptions(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetSequenceRelList(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetIndexes(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetConstraints().AnyTimes()
	introspector.EXPECT().GetForeignKeys().AnyTimes()
	introspector.EXPECT().GetFunctions().Return([]live.FunctionEntry{
		live.FunctionEntry{
			Oid:      live.Oid{1},
			Schema:   "public",
			Name:     "increment1",
			Return:   "integer",
			Type:     "normal",
			Language: "plpgsql",
			Source:   body,
		},
		live.FunctionEntry{
			Oid:      live.Oid{2},
			Schema:   "public",
			Name:     "increment2",
			Return:   "integer",
			Type:     "normal",
			Language: "plpgsql",
			Source:   body,
		},
		live.FunctionEntry{
			Oid:      live.Oid{3},
			Schema:   "public",
			Name:     "increment3",
			Return:   "integer",
			Type:     "normal",
			Language: "plpgsql",
			Source:   body,
		},
		live.FunctionEntry{
			Oid:      live.Oid{4},
			Schema:   "public",
			Name:     "increment4",
			Return:   "integer",
			Type:     "normal",
			Language: "plpgsql",
			Source:   body,
		},
	}, nil)
	// array type and argument names
	introspector.EXPECT().GetFunctionArgs(live.Oid{1}).Return([]live.FunctionArgEntry{
		{"arg1", "integer[]"},
		{"arg2", "uuid[]"},
	}, nil)
	// array type and no argument names
	introspector.EXPECT().GetFunctionArgs(live.Oid{2}).Return([]live.FunctionArgEntry{
		{"", "integer[]"},
		{"", "uuid[]"},
	}, nil)
	// array type and mixed argument names
	introspector.EXPECT().GetFunctionArgs(live.Oid{3}).Return([]live.FunctionArgEntry{
		{"arg1", "integer[]"},
		{"", "uuid[]"},
	}, nil)
	introspector.EXPECT().GetFunctionArgs(live.Oid{4}).Return([]live.FunctionArgEntry{
		{"", "integer[]"},
		{"arg1", "uuid[]"},
	}, nil)
	introspector.EXPECT().GetTriggers().AnyTimes()
	introspector.EXPECT().GetViews().AnyTimes()
	introspector.EXPECT().GetTablePerms().AnyTimes()
	introspector.EXPECT().GetSequencePerms(gomock.Any()).AnyTimes()

	schema := commonExtract(introspector, PG_8_0)
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
	ctrl := gomock.NewController(t)
	introspector := live.NewMockIntrospector(ctrl)

	introspector.EXPECT().GetSchemaOwner(gomock.Any()).AnyTimes()
	introspector.EXPECT().GetTableList().Return([]live.TableEntry{
		{Schema: "public", Table: "test"},
	}, nil)
	introspector.EXPECT().GetColumns("public", "test").Return([]live.ColumnEntry{
		{Name: "name", AttrType: "text[]"},
	}, nil)
	introspector.EXPECT().GetTableStorageOptions(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetSequenceRelList(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetIndexes(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetConstraints().AnyTimes()
	introspector.EXPECT().GetForeignKeys().AnyTimes()
	introspector.EXPECT().GetFunctions().AnyTimes()
	introspector.EXPECT().GetFunctionArgs(gomock.Any()).AnyTimes()
	introspector.EXPECT().GetTriggers().AnyTimes()
	introspector.EXPECT().GetViews().AnyTimes()
	introspector.EXPECT().GetTablePerms().AnyTimes()
	introspector.EXPECT().GetSequencePerms(gomock.Any()).AnyTimes()

	schema := commonExtract(introspector, PG_8_0)
	assert.Equal(t, "text[]", schema.Tables[0].Columns[0].Type)
}

func TestOperations_ExtractSchema_FKReferentialConstraints(t *testing.T) {
	ctrl := gomock.NewController(t)
	introspector := live.NewMockIntrospector(ctrl)

	// CREATE TABLE dummy (foo int, bar varchar(32), PRIMARY KEY (foo, bar));
	// CREATE TABLE test (
	// 	id int PRIMARY KEY,
	// 	foo int,
	// 	bar varchar(32),
	// 	FOREIGN KEY (foo, bar) REFERENCES dummy (foo, bar)
	// 		ON UPDATE NO ACTION
	// 		ON DELETE SET NULL
	// );

	introspector.EXPECT().GetSchemaOwner(gomock.Any()).AnyTimes()
	introspector.EXPECT().GetTableList().Return([]live.TableEntry{
		{Schema: "public", Table: "dummy"},
		{Schema: "public", Table: "test"},
	}, nil)
	introspector.EXPECT().GetColumns("public", "dummy").Return([]live.ColumnEntry{
		{Name: "feux", AttrType: "integer"},
		{Name: "barf", AttrType: "character varying(32)"},
	}, nil)
	introspector.EXPECT().GetColumns("public", "test").Return([]live.ColumnEntry{
		{Name: "id", AttrType: "integer"},
		{Name: "foo", AttrType: "integer"},
		{Name: "bar", AttrType: "character varying(32)"},
	}, nil)
	introspector.EXPECT().GetTableStorageOptions(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetSequenceRelList(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetIndexes(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetConstraints().Return([]live.ConstraintEntry{
		{Schema: "public", Table: "dummy", Name: "dummy_pkey", Type: "p", Columns: []string{"foo", "bar"}},
		{Schema: "public", Table: "test", Name: "test_pkey", Type: "p", Columns: []string{"id"}},
	}, nil)
	introspector.EXPECT().GetForeignKeys().Return([]live.ForeignKeyEntry{
		live.ForeignKeyEntry{
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
	}, nil)
	introspector.EXPECT().GetFunctions().AnyTimes()
	introspector.EXPECT().GetFunctionArgs(gomock.Any()).AnyTimes()
	introspector.EXPECT().GetTriggers().AnyTimes()
	introspector.EXPECT().GetViews().AnyTimes()
	introspector.EXPECT().GetTablePerms().AnyTimes()
	introspector.EXPECT().GetSequencePerms(gomock.Any()).AnyTimes()

	schema := commonExtract(introspector, PG_8_0)
	assert.Equal(t, []*model.ForeignKey{
		&model.ForeignKey{
			ConstraintName: "test_foo_fkey",
			Columns:        model.DelimitedList{"foo", "bar"},
			ForeignSchema:  "public",
			ForeignTable:   "dummy",
			ForeignColumns: model.DelimitedList{"feux", "barf"},
			OnUpdate:       model.ForeignKeyActionNoAction,
			OnDelete:       model.ForeignKeyActionSetNull,
		},
	}, schema.Tables[1].ForeignKeys)
	// these should _not_ be omitted in this case, because it's a separate element
	assert.Equal(t, "integer", schema.Tables[1].Columns[1].Type)
	assert.Equal(t, "character varying(32)", schema.Tables[1].Columns[2].Type)
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

	ctrl := gomock.NewController(t)
	introspector := live.NewMockIntrospector(ctrl)

	introspector.EXPECT().GetSchemaOwner(gomock.Any()).AnyTimes()
	introspector.EXPECT().GetTableList().Return([]live.TableEntry{
		{Schema: "public", Table: "user"},
	}, nil)
	introspector.EXPECT().GetColumns("public", "user").Return([]live.ColumnEntry{
		{Name: "user_id", AttrType: "integer", Default: "nextval('test_seq')"},
		{Name: "user_name", AttrType: "varchar(100)"},
	}, nil)
	introspector.EXPECT().GetTableStorageOptions(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetSequenceRelList("public", []string{"test_seq"}).Return([]live.SequenceRelEntry{
		{Name: "blah", Owner: "owner"},
	}, nil)
	introspector.EXPECT().GetSequencesForRel("public", "test_seq").Return([]live.SequenceEntry{
		{Start: util.Ptr(1), Increment: util.Ptr(1), Cache: util.Ptr(1), Max: util.Ptr(15)},
	}, nil)
	introspector.EXPECT().GetSequencesForRel("public", "blah").Return([]live.SequenceEntry{
		{Cache: util.Ptr(5), Min: util.Ptr(3), Max: util.Ptr(10)},
	}, nil)
	introspector.EXPECT().GetIndexes(gomock.Any(), gomock.Any()).AnyTimes()
	introspector.EXPECT().GetConstraints().Return([]live.ConstraintEntry{
		{Schema: "public", Table: "user", Name: "user_pkey", Type: "p", Columns: []string{"user_id"}},
	}, nil)
	introspector.EXPECT().GetForeignKeys().AnyTimes()
	introspector.EXPECT().GetFunctions().AnyTimes()
	introspector.EXPECT().GetFunctionArgs(gomock.Any()).AnyTimes()
	introspector.EXPECT().GetTriggers().AnyTimes()
	introspector.EXPECT().GetViews().AnyTimes()
	introspector.EXPECT().GetTablePerms().AnyTimes()
	introspector.EXPECT().GetSequencePerms(gomock.Any()).AnyTimes()

	schema := commonExtract(introspector, PG_8_0)
	// Test that int sequences become serials
	// TODO(go,3) this doesn't feel right - does an int/nextval column have different semantics than a serial type?
	//            It feels wrong that we simply don't extract the sequence. I'd rather extract it as-is and let the
	//            user sort it all out. Will maintain v1 behavior for now though.

	assert.Equal(t, "serial", schema.Tables[0].Columns[0].Type)
	assert.Equal(t, []*model.Sequence{
		// test_seq SHOULD NOT be extracted (see `TestOperations_ExtractSchema_DoNotExtractSequenceFromSerial`)
		// blah SHOULD be extracted because it's not linked to
		&model.Sequence{
			Name:  "blah",
			Owner: "owner",
			Cache: util.Ptr(5),
			Min:   util.Ptr(3),
			Max:   util.Ptr(10),
		},
	}, schema.Sequences)
}

func commonExtract(introspector *live.MockIntrospector, version live.VersionNum) *model.Schema {
	ops := pgsql8.GlobalOperations
	origCF := ops.ConnectionFactory
	origIF := ops.IntrospectorFactory
	defer func() {
		ops.ConnectionFactory = origCF
		ops.IntrospectorFactory = origIF
	}()

	ops.ConnectionFactory = &live.ConstantConnectionFactory{
		Connection: &live.NullConnection{},
	}
	ops.IntrospectorFactory = &live.ConstantIntrospectorFactory{
		Introspector: introspector,
	}
	introspector.EXPECT().GetServerVersion().Return(version, nil)

	doc := ops.ExtractSchema("", 0, "", "", "")
	return doc.Schemas[0]
}
