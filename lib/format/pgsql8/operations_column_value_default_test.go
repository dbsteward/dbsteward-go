package pgsql8

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/ir"
)

func TestOperations_ColumnValueDefault_NullReturnsNull(t *testing.T) {
	val := getColumnValueDefault(&ir.Column{
		Name: "foo",
		Type: "text",
	}, &ir.DataCol{
		Null: true,
		Text: "asdf",
	})
	assert.Equal(t, "NULL", val, `Expected NULL if null="true" is specified`)
}

func TestOperations_ColumnValueDefault_EmptyReturnsEmpty(t *testing.T) {
	val := getColumnValueDefault(&ir.Column{
		Name: "foo",
		Type: "text",
	}, &ir.DataCol{
		Empty: true,
		Text:  "asdf",
	})
	assert.Equal(t, "E''", val, `Expected "E''" if empty="true" is specified`)
}

func TestOperations_ColumnValueDefault_SqlReturnsWrapped(t *testing.T) {
	val := getColumnValueDefault(&ir.Column{
		Name: "foo",
		Type: "text",
	}, &ir.DataCol{
		Sql:  true,
		Text: "some_function()",
	})
	assert.Equal(t, "(some_function())", val, `Expected literal column value wrapped in parens if sql="true" is specified`)
}

func TestOperations_ColumnValueDefault_SqlDefaultReturnsWrapped(t *testing.T) {
	val := getColumnValueDefault(&ir.Column{
		Name: "foo",
		Type: "text",
	}, &ir.DataCol{
		Sql:  true,
		Text: "DEFAULT",
	})
	assert.Equal(t, "DEFAULT", val, `Expected un-paren-wrapped DEFAULT if sql="true" is specified`)
}

func TestOperations_ColumnValueDefault_UsesDefaultIfEmpty(t *testing.T) {
	val := getColumnValueDefault(&ir.Column{
		Name:    "foo",
		Type:    "text",
		Default: "asdf",
	}, &ir.DataCol{})
	assert.Equal(t, "asdf", val, `Expected column default if data was empty`)
}

func TestOperations_ColumnValueDefault_UsesLiteralForInt(t *testing.T) {
	val := getColumnValueDefault(&ir.Column{
		Name: "foo",
		Type: "int",
	}, &ir.DataCol{
		Text: "42",
	})
	assert.Equal(t, "42", val, `Expected literal int value for integers`)
}

func TestOperations_ColumnValueDefaultQuotesStrings(t *testing.T) {
	defer resetGlobalDBSteward()
	val := getColumnValueDefault(&ir.Column{
		Name: "foo",
		Type: "text",
	}, &ir.DataCol{
		Text: "asdf",
	})
	assert.Equal(t, "E'asdf'", val, `Expected quoted string value for text`)
}

func getColumnValueDefault(def *ir.Column, data *ir.DataCol) string {
	doc := &ir.Definition{
		Schemas: []*ir.Schema{
			&ir.Schema{
				Name: "test_schema",
				Tables: []*ir.Table{
					&ir.Table{
						Name:       "test_table",
						PrimaryKey: []string{def.Name},
						Columns:    []*ir.Column{def},
						Rows: &ir.DataRows{
							Columns: []string{def.Name},
							Rows: []*ir.DataRow{
								&ir.DataRow{
									Columns: []*ir.DataCol{data},
								},
							},
						},
					},
				},
			},
		},
	}
	lib.GlobalDBSteward.NewDatabase = doc
	schema := doc.Schemas[0]
	table := schema.Tables[0]

	ops := GlobalOperations
	ops.EscapeStringValues = true

	// TODO(go,nth) can we do this without also testing GetValueSql?
	toVal := columnValueDefault(schema, table, def.Name, data)
	return toVal.GetValueSql(ops.GetQuoter())
}
