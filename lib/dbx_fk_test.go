package lib_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
)

func TestDBX_ResolveForeignKey_InheritedColumn(t *testing.T) {
	// NOTE: In v1 this was tests/pgsql8/FKeyToInheritedTableTest.php
	doc := &model.Definition{
		Schemas: []*model.Schema{
			&model.Schema{
				Name: "test",
				Tables: []*model.Table{
					&model.Table{
						Name:       "parent",
						PrimaryKey: model.DelimitedList{"foo"},
						Columns: []*model.Column{
							{Name: "foo", Type: "varchar(255)"},
						},
					},
					&model.Table{
						Name:           "child",
						PrimaryKey:     model.DelimitedList{"foo"},
						InheritsSchema: "test",
						InheritsTable:  "parent",
						Columns: []*model.Column{
							{Name: "bar", Type: "varchar(255)"},
						},
					},
				},
			},
			&model.Schema{
				Name: "other",
				Tables: []*model.Table{
					&model.Table{
						Name:       "baz",
						PrimaryKey: model.DelimitedList{"footoo"},
						Columns: []*model.Column{
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
