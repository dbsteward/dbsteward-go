package pgsql8

import (
	"testing"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/stretchr/testify/assert"
)

func TestIndex_GetTableIndexes_DuplicateIndexNameError(t *testing.T) {
	schema := &ir.Schema{
		Name: "public",
		Tables: []*ir.Table{
			&ir.Table{
				Name:       "table1",
				PrimaryKey: []string{"col1"},
				Columns: []*ir.Column{
					{Name: "col1", Type: "int"},
				},
				Indexes: []*ir.Index{
					{Name: "index1", Dimensions: []*ir.IndexDim{{"index1_1", false, "col1"}}},
					{Name: "index1", Dimensions: []*ir.IndexDim{{"index1_1", false, "col1"}}},
				},
			},
		},
	}

	_, err := getTableIndexes(schema, schema.Tables[0])
	assert.Error(t, err, "Expected an error because the table had duplicate index names")
}
