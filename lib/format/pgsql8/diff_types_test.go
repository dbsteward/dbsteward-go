package pgsql8_test

import (
	"testing"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sqltest"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/stretchr/testify/assert"

	"github.com/dbsteward/dbsteward/lib/model"
)

func TestDiffTypes_DiffTypes_RecreateDependentFunctions(t *testing.T) {
	oldSchema := &model.Schema{
		Name: "test",
		Functions: []*model.Function{
			&model.Function{
				Name:    "test_arch_type_in_return",
				Returns: "test.arch_type",
				Definitions: []*model.FunctionDefinition{
					&model.FunctionDefinition{
						Language:  "plpgsql",
						SqlFormat: model.SqlFormatPgsql8,
						Text:      "BEGIN\nRETURN 1;\nEND",
					},
				},
			},
			&model.Function{
				Name:    "test_arch_type_in_param",
				Returns: "bigint",
				Parameters: []*model.FunctionParameter{
					{Name: "testparam", Type: "test.arch_type"},
				},
				Definitions: []*model.FunctionDefinition{
					&model.FunctionDefinition{
						Language:  "plpgsql",
						SqlFormat: model.SqlFormatPgsql8,
						Text:      "BEGIN\nRETURN 1;\nEND",
					},
				},
			},
		},
		Types: []*model.DataType{
			&model.DataType{
				Name: "arch_type",
				Kind: model.DataTypeKindComposite,
				CompositeFields: []*model.DataTypeCompositeField{
					{Name: "uh_phrasing", Type: "text"},
					{Name: "boom_phrasing", Type: "text"},
				},
			},
		},
	}

	newSchema := &model.Schema{
		Name: "test",
		Functions: []*model.Function{
			&model.Function{
				Name:    "test_arch_type_in_return",
				Returns: "test.arch_type",
				Definitions: []*model.FunctionDefinition{
					&model.FunctionDefinition{
						Language:  "plpgsql",
						SqlFormat: model.SqlFormatPgsql8,
						Text:      "BEGIN\nRETURN 1;\nEND",
					},
				},
			},
			&model.Function{
				Name:    "test_arch_type_in_param",
				Returns: "bigint",
				Parameters: []*model.FunctionParameter{
					{Name: "testparam", Type: "test.arch_type"},
				},
				Definitions: []*model.FunctionDefinition{
					&model.FunctionDefinition{
						Language:  "plpgsql",
						SqlFormat: model.SqlFormatPgsql8,
						Text:      "BEGIN\nRETURN 1;\nEND",
					},
				},
			},
		},
		Types: []*model.DataType{
			&model.DataType{
				Name: "arch_type",
				Kind: model.DataTypeKindComposite,
				CompositeFields: []*model.DataTypeCompositeField{
					{Name: "uh_phrasing", Type: "text"},
					{Name: "boom_phrasing", Type: "text"},
					{Name: "ummmm_phrasing", Type: "text"},
				},
			},
		},
	}

	ofs := &sqltest.RecordingOfs{
		StripComments: true,
	}

	pgsql8.NewDiffTypes().DiffTypes(ofs, oldSchema, newSchema)
	assert.Equal(t, []output.ToSql{
		&sql.FunctionDrop{
			Function: sql.FunctionRef{"test", "test_arch_type_in_return", []string{}},
		},
		&sql.FunctionDrop{
			Function: sql.FunctionRef{"test", "test_arch_type_in_param", []string{"test.arch_type"}},
		},
		&sql.TypeDrop{
			Type: sql.TypeRef{"test", "arch_type"},
		},
		&sql.TypeCompositeCreate{
			Type: sql.TypeRef{"test", "arch_type"},
			Fields: []sql.TypeCompositeCreateField{
				{Name: "uh_phrasing", Type: "text"},
				{Name: "boom_phrasing", Type: "text"},
				{Name: "ummmm_phrasing", Type: "text"},
			},
		},
		&sql.FunctionCreate{
			Function:   sql.FunctionRef{"test", "test_arch_type_in_return", []string{}},
			Returns:    "test.arch_type",
			Language:   "plpgsql",
			Definition: "BEGIN\nRETURN 1;\nEND",
		},
		&sql.FunctionCreate{
			Function:   sql.FunctionRef{"test", "test_arch_type_in_param", []string{"testparam test.arch_type"}},
			Returns:    "bigint",
			Language:   "plpgsql",
			Definition: "BEGIN\nRETURN 1;\nEND",
		},
	}, ofs.Sql)
}
