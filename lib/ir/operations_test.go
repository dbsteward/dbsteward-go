package ir

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDBX_ResolveForeignKey_InheritedColumn(t *testing.T) {
	// NOTE: In v1 this was tests/pgsql8/FKeyToInheritedTableTest.php
	doc := &Definition{
		Schemas: []*Schema{
			{
				Name: "test",
				Tables: []*Table{
					{
						Name:       "parent",
						PrimaryKey: []string{"foo"},
						Columns: []*Column{
							{Name: "foo", Type: "varchar(255)"},
						},
					},
					{
						Name:           "child",
						PrimaryKey:     []string{"foo"},
						InheritsSchema: "test",
						InheritsTable:  "parent",
						Columns: []*Column{
							{Name: "bar", Type: "varchar(255)"},
						},
					},
				},
			},
			{
				Name: "other",
				Tables: []*Table{
					{
						Name:       "baz",
						PrimaryKey: []string{"footoo"},
						Columns: []*Column{
							{Name: "footoo", ForeignSchema: "test", ForeignTable: "child", ForeignColumn: "foo"},
						},
					},
				},
			},
		},
	}
	schema := doc.Schemas[1]
	table := schema.Tables[0]
	column := table.Columns[0]

	fkey, err := doc.ResolveForeignKeyColumn(schema, table, column)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, doc.Schemas[0], fkey.Schema)
	assert.Equal(t, doc.Schemas[0].Tables[1], fkey.Table)
	assert.Equal(t, doc.Schemas[0].Tables[0].Columns[0], fkey.Columns[0])
}
