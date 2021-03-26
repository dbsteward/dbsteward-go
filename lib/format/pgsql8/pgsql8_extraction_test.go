package pgsql8_test

import (
	"testing"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/live"
	"github.com/dbsteward/dbsteward/lib/model"
)

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
			Name:     "name",
			AttrType: "text",
			Position: 1,
		},
	}, nil)
	introspector.EXPECT().GetIndexes("public", "test").Return([]live.IndexEntry{
		live.IndexEntry{
			Name:       "lower_idx",
			Dimensions: []string{"lower(name)"},
		},
	}, nil)

	schema := commonExtract(introspector)
	assert.Equal(t, "lower_idx_1", schema.Tables[0].Indexes[0].Dimensions[0].Name)
	assert.Equal(t, "lower(name)", schema.Tables[0].Indexes[0].Dimensions[0].Value)
	// TODO(feat) assert that .Sql = true
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
