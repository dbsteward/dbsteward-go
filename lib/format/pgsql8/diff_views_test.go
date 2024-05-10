package pgsql8

import (
	"testing"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/stretchr/testify/assert"
)

func oldSingleView() *ir.Definition {
	return &ir.Definition{
		Schemas: []*ir.Schema{
			{
				Name: "testSchema",
				Views: []*ir.View{
					{
						Name: "testView",
						Queries: []*ir.ViewQuery{
							{
								SqlFormat: ir.SqlFormatPgsql8,
								Text:      "SELECT * FROM someTable",
							},
						},
					},
				},
			},
		},
	}
}

func newSingleView() *ir.Definition {
	return &ir.Definition{
		Schemas: []*ir.Schema{
			{
				Name: "testSchema",
				Views: []*ir.View{
					{
						Name: "testView",
						Queries: []*ir.ViewQuery{
							{
								SqlFormat: ir.SqlFormatPgsql8,
								Text:      "SELECT * FROM someTable LIMIT 10",
							},
						},
					},
				},
			},
		},
	}
}

func TestCreateViewsOrdered(t *testing.T) {
	dbs := lib.NewDBSteward()
	q := defaultQuoter(dbs)
	ofs := output.NewAnnotationStrippingSegmenter(q)
	err := createViewsOrdered(dbs, ofs, oldSingleView(), newSingleView())
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(
		t,
		[]output.ToSql{
			&sql.ViewCreate{
				View:  sql.ViewRef{Schema: "testSchema", View: "testView"},
				Query: "SELECT * FROM someTable LIMIT 10",
			},
		},
		ofs.Body,
	)
}
