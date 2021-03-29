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

func TestOperations_ExtractSchema_FunctionalIndex(t *testing.T) {
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
	}, nil)

	schema := commonExtract(introspector)

	// TODO(feat) test changing Using
	// TODO(feat) test conditional index
	// TODO(feat) test unique index
	// TODO(feat) assert that .Sql = true
	expected := &model.Index{
		Name:  "testidx",
		Using: "btree",
		Dimensions: []*model.IndexDim{
			{Name: "testidx_1", Value: "lower(col1)"},
			{Name: "testidx_2", Value: "col2"},
			{Name: "testidx_3", Value: "(col1 || ';;'::text)"},
			{Name: "testidx_4", Value: "col3"},
			{Name: "testidx_5", Value: "\"overlay\"(btrim(col2), 'x'::text, 2)"},
		},
	}
	// test the full slice of indexes to ensure we don't do something weird like duplicate/split the index
	assert.Equal(t, []*model.Index{expected}, schema.Tables[0].Indexes)
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
