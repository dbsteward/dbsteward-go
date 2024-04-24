package pgsql8

import (
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util/testutil"
)

// Tests that functions referenced as default values for columns
// 1) do not result in build failures
// 2) generate sane SQL on build
// TODO(go,3) cut down on the surface area of this test. is there something smaller than BuildSchema?
func TestColumnDefaultIsFunction(t *testing.T) {
	doc := &ir.Definition{
		Schemas: []*ir.Schema{
			{
				Name: "dbsteward",
				Functions: []*ir.Function{
					{Name: "test"},
				},
			},
			{
				Name: "hotel",
				Tables: []*ir.Table{
					{
						Name:       "rate",
						PrimaryKey: []string{"rate_id"},
						Columns: []*ir.Column{
							{Name: "rate_id", Type: "integer", Nullable: false},
							{Name: "rate_group_id", Nullable: false, ForeignTable: "rate_group"},
							{Name: "rate_name", Type: "character varying(120)", Nullable: true},
							{Name: "rate_value", Type: "numeric", Nullable: true},
						},
					},
					{
						Name:       "rate_group",
						PrimaryKey: []string{"rate_group_id"},
						Columns: []*ir.Column{
							{Name: "rate_group_id", Type: "integer", Nullable: false, Default: "dbsteward.test()"},
							{Name: "rate_group_name", Type: "character varying(100)", Nullable: true},
							{Name: "rate_group_enabled", Type: "boolean", Nullable: false, Default: "true"},
						},
					},
				},
			},
		},
	}

	tableDep := lib.GlobalDBX.TableDependencyOrder(doc)

	ctrl := gomock.NewController(t)
	ofs := output.NewMockOutputFileSegmenter(ctrl)

	rateIdRef := sql.ColumnRef{"hotel", "rate", "rate_id"}
	rateGroupIdRef := sql.ColumnRef{"hotel", "rate", "rate_group_id"}
	rateGroupGroupIdRef := sql.ColumnRef{"hotel", "rate_group", "rate_group_id"}
	rateGroupEnabledRef := sql.ColumnRef{"hotel", "rate_group", "rate_group_enabled"}

	actual := []output.ToSql{}
	ofs.EXPECT().WriteSql(gomock.Any()).
		AnyTimes().
		Do(func(items ...output.ToSql) {
			actual = append(actual, items...)
		})

	expected := []output.ToSql{
		&sql.ColumnSetNull{rateIdRef, false},
		&sql.ColumnSetNull{rateGroupIdRef, false},
		&sql.ColumnSetDefault{rateGroupGroupIdRef, sql.RawSql("dbsteward.test()")},
		&sql.ColumnSetNull{rateGroupGroupIdRef, false},
		&sql.ColumnSetDefault{rateGroupEnabledRef, sql.RawSql("true")},
		&sql.ColumnSetNull{rateGroupEnabledRef, false},
	}

	lib.GlobalDBSteward.NewDatabase = doc
	buildSchema(doc, ofs, tableDep)

	testutil.AssertContainsSubseq(t, actual, expected)
}
