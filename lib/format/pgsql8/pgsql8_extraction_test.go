package pgsql8_test

import (
	"testing"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/live"
	"github.com/dbsteward/dbsteward/lib/model"
)

// TODO(go,3) is there a way to make this set of tests a whole lot less annoying?
// TODO(go,pgsql) the v1 ExtractionTest tested what is now ExtractSchema, Introspector, Connection, _and_ postgres
//                but this only tests ExtractSchema. We still should test the other layers, and come up with a story
//                around e2e testing with a real db connection.

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
			Name: "testidx2",
			Dimensions: []string{"col1", "col2", "col3"},
		},
		live.IndexEntry{
			Name: "testidx3",
			Dimensions: []string{"col2", "col1", "col3"},
		},
	}, nil)

	schema := commonExtract(introspector)

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
			Schema: "public",
			Table: "test",
			Name: "test_constraint",
			Type: "u",
			Columns: []string{"col2", "col3", "col4"},
		},
	}, nil)

	schema := commonExtract(introspector)

	// compound constraints should not set individual column uniqueness
	assert.False(t, schema.Tables[0].Columns[1].Unique)
	assert.False(t, schema.Tables[0].Columns[2].Unique)
	assert.False(t, schema.Tables[0].Columns[3].Unique)

	// TODO(go,pgsql) why is this quoted, it shouldn't be quoted unless we turn it on.... right?
	assert.Equal(t, []*model.Constraint{
		{Name: "test_constraint", Type: model.ConstraintTypeUnique, Definition: `("col2", "col3", "col4")`},
	}, schema.Tables[0].Constraints)
}

func commonExtract(introspector *live.MockIntrospector) *model.Schema {
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
	doc := ops.ExtractSchema("", 0, "", "", "")
	return doc.Schemas[0]
}
