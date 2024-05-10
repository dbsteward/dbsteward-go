package pgsql8

import (
	"testing"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/stretchr/testify/assert"
)

func TestDiffTypes_DiffTypes_RecreateDependentFunctions(t *testing.T) {
	oldSchema := &ir.Schema{
		Name: "test",
		Functions: []*ir.Function{
			{
				Name:    "test_arch_type_in_return",
				Returns: "test.arch_type",
				Definitions: []*ir.FunctionDefinition{
					{
						Language:  "plpgsql",
						SqlFormat: ir.SqlFormatPgsql8,
						Text:      "BEGIN\nRETURN 1;\nEND",
					},
				},
			},
			{
				Name:    "test_arch_type_in_param",
				Returns: "bigint",
				Parameters: []*ir.FunctionParameter{
					{Name: "testparam", Type: "test.arch_type"},
				},
				Definitions: []*ir.FunctionDefinition{
					{
						Language:  "plpgsql",
						SqlFormat: ir.SqlFormatPgsql8,
						Text:      "BEGIN\nRETURN 1;\nEND",
					},
				},
			},
		},
		Types: []*ir.TypeDef{
			{
				Name: "arch_type",
				Kind: ir.DataTypeKindComposite,
				CompositeFields: []ir.DataTypeCompositeField{
					{Name: "uh_phrasing", Type: "text"},
					{Name: "boom_phrasing", Type: "text"},
				},
			},
		},
	}

	newSchema := &ir.Schema{
		Name: "test",
		Functions: []*ir.Function{
			{
				Name:    "test_arch_type_in_return",
				Returns: "test.arch_type",
				Definitions: []*ir.FunctionDefinition{
					{
						Language:  "plpgsql",
						SqlFormat: ir.SqlFormatPgsql8,
						Text:      "BEGIN\nRETURN 1;\nEND",
					},
				},
			},
			{
				Name:    "test_arch_type_in_param",
				Returns: "bigint",
				Parameters: []*ir.FunctionParameter{
					{Name: "testparam", Type: "test.arch_type"},
				},
				Definitions: []*ir.FunctionDefinition{
					{
						Language:  "plpgsql",
						SqlFormat: ir.SqlFormatPgsql8,
						Text:      "BEGIN\nRETURN 1;\nEND",
					},
				},
			},
		},
		Types: []*ir.TypeDef{
			{
				Name: "arch_type",
				Kind: ir.DataTypeKindComposite,
				CompositeFields: []ir.DataTypeCompositeField{
					{Name: "uh_phrasing", Type: "text"},
					{Name: "boom_phrasing", Type: "text"},
					{Name: "ummmm_phrasing", Type: "text"},
				},
			},
		},
	}

	ops := NewOperations(DefaultConfig).(*Operations)
	ofs := output.NewAnnotationStrippingSegmenter(defaultQuoter(DefaultConfig))
	err := diffTypes(DefaultConfig, newDiff(ops, defaultQuoter(DefaultConfig)), ofs, oldSchema, newSchema)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, []output.ToSql{
		&sql.FunctionDrop{
			Function: sql.FunctionRef{
				Schema:   "test",
				Function: "test_arch_type_in_return",
				Params:   []string{},
			},
		},
		&sql.FunctionDrop{
			Function: sql.FunctionRef{
				Schema:   "test",
				Function: "test_arch_type_in_param",
				Params:   []string{"test.arch_type"},
			},
		},
		&sql.TypeDrop{
			Type: sql.TypeRef{Schema: "test", Type: "arch_type"},
		},
		&sql.TypeCompositeCreate{
			Type: sql.TypeRef{Schema: "test", Type: "arch_type"},
			Fields: []sql.TypeCompositeCreateField{
				{Name: "uh_phrasing", Type: "text"},
				{Name: "boom_phrasing", Type: "text"},
				{Name: "ummmm_phrasing", Type: "text"},
			},
		},
		&sql.FunctionCreate{
			Function: sql.FunctionRef{
				Schema:   "test",
				Function: "test_arch_type_in_return",
				Params:   []string{},
			},
			Returns:    "test.arch_type",
			Language:   "plpgsql",
			Definition: "BEGIN\nRETURN 1;\nEND",
		},
		&sql.FunctionCreate{
			Function: sql.FunctionRef{
				Schema:   "test",
				Function: "test_arch_type_in_param",
				Params:   []string{"testparam test.arch_type"},
			},
			Returns:    "bigint",
			Language:   "plpgsql",
			Definition: "BEGIN\nRETURN 1;\nEND",
		},
	}, ofs.Body)
}
