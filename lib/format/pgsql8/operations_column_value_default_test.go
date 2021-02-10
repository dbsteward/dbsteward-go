package pgsql8_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8"
	"github.com/dbsteward/dbsteward/lib/model"
)

func TestOperations_ColumnValueDefault_NullReturnsNull(t *testing.T) {
	val := getColumnValueDefault(&model.Column{
		Name: "foo",
		Type: "text",
	}, &model.DataCol{
		Null: true,
		Text: "asdf",
	})
	assert.Equal(t, "NULL", val, `Expected NULL if null="true" is specified`)
}

func TestOperations_ColumnValueDefault_EmptyReturnsEmpty(t *testing.T) {
	val := getColumnValueDefault(&model.Column{
		Name: "foo",
		Type: "text",
	}, &model.DataCol{
		Empty: true,
		Text:  "asdf",
	})
	assert.Equal(t, "E''", val, `Expected "E''" if empty="true" is specified`)
}

func TestOperations_ColumnValueDefault_SqlReturnsWrapped(t *testing.T) {
	val := getColumnValueDefault(&model.Column{
		Name: "foo",
		Type: "text",
	}, &model.DataCol{
		Sql:  true,
		Text: "some_function()",
	})
	assert.Equal(t, "(some_function())", val, `Expected literal column value wrapped in parens if sql="true" is specified`)
}

func TestOperations_ColumnValueDefault_SqlDefaultReturnsWrapped(t *testing.T) {
	val := getColumnValueDefault(&model.Column{
		Name: "foo",
		Type: "text",
	}, &model.DataCol{
		Sql:  true,
		Text: "DEFAULT",
	})
	assert.Equal(t, "DEFAULT", val, `Expected un-paren-wrapped DEFAULT if sql="true" is specified`)
}

func TestOperations_ColumnValueDefault_UsesDefaultIfEmpty(t *testing.T) {
	val := getColumnValueDefault(&model.Column{
		Name:    "foo",
		Type:    "text",
		Default: "asdf",
	}, &model.DataCol{})
	assert.Equal(t, "asdf", val, `Expected column default if data was empty`)
}

func TestOperations_ColumnValueDefault_UsesLiteralForInt(t *testing.T) {
	val := getColumnValueDefault(&model.Column{
		Name: "foo",
		Type: "int",
	}, &model.DataCol{
		Text: "42",
	})
	assert.Equal(t, "42", val, `Expected literal int value for integers`)
}

func TestOperations_ColumnValueDefaultQuotesStrings(t *testing.T) {
	val := getColumnValueDefault(&model.Column{
		Name: "foo",
		Type: "text",
	}, &model.DataCol{
		Text: "asdf",
	})
	assert.Equal(t, "E'asdf'", val, `Expected quoted string value for text`)
}

func getColumnValueDefault(def *model.Column, data *model.DataCol) string {
	doc := &model.Definition{
		Schemas: []*model.Schema{
			&model.Schema{
				Name: "test_schema",
				Tables: []*model.Table{
					&model.Table{
						Name:       "test_table",
						PrimaryKey: model.DelimitedList{def.Name},
						Columns:    []*model.Column{def},
						Rows: &model.DataRows{
							Columns: model.DelimitedList{def.Name},
							Rows: []*model.DataRow{
								&model.DataRow{
									Columns: []*model.DataCol{data},
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

	ops := pgsql8.NewOperations()
	ops.EscapeStringValues = true

	// TODO(go,nth) can we do this without also testing GetValueSql?
	toVal := ops.ColumnValueDefault(schema, table, def.Name, data)
	return toVal.GetValueSql(ops)
}
