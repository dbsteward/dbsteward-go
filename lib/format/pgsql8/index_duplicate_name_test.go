package pgsql8_test

import (
	"testing"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/stretchr/testify/assert"
)

func TestIndex_GetTableIndexes_DuplicateIndexNameError(t *testing.T) {
	schema := &model.Schema{
		Name: "public",
		Tables: []*model.Table{
			&model.Table{
				Name:       "table1",
				PrimaryKey: []string{"col1"},
				Columns: []*model.Column{
					{Name: "col1", Type: "int"},
				},
				Indexes: []*model.Index{
					{Name: "index1", Dimensions: []*model.IndexDim{{"index1_1", false, "col1"}}},
					{Name: "index1", Dimensions: []*model.IndexDim{{"index1_1", false, "col1"}}},
				},
			},
		},
	}

	_, err := pgsql8.GlobalIndex.GetTableIndexes(schema, schema.Tables[0])
	assert.Error(t, err, "Expected an error because the table had duplicate index names")
}
