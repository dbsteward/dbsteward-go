package lib_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/ir"
)

func TestDBX_ResolveForeignKey_InheritedColumn(t *testing.T) {
	// NOTE: In v1 this was tests/pgsql8/FKeyToInheritedTableTest.php
	doc := &ir.Definition{
		Schemas: []*ir.Schema{
			&ir.Schema{
				Name: "test",
				Tables: []*ir.Table{
					&ir.Table{
						Name:       "parent",
						PrimaryKey: []string{"foo"},
						Columns: []*ir.Column{
							{Name: "foo", Type: "varchar(255)"},
						},
					},
					&ir.Table{
						Name:           "child",
						PrimaryKey:     []string{"foo"},
						InheritsSchema: "test",
						InheritsTable:  "parent",
						Columns: []*ir.Column{
							{Name: "bar", Type: "varchar(255)"},
						},
					},
				},
			},
			&ir.Schema{
				Name: "other",
				Tables: []*ir.Table{
					&ir.Table{
						Name:       "baz",
						PrimaryKey: []string{"footoo"},
						Columns: []*ir.Column{
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

	fkey := lib.NewDBX().ResolveForeignKeyColumn(doc, schema, table, column)
	assert.Equal(t, doc.Schemas[0], fkey.Schema)
	assert.Equal(t, doc.Schemas[0].Tables[1], fkey.Table)
	assert.Equal(t, doc.Schemas[0].Tables[0].Columns[0], fkey.Columns[0])
}
