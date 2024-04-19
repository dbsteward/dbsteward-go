package lib_test

import (
	"strings"
	"testing"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/stretchr/testify/assert"
)

func TestXmlParser_CompositeDoc_InheritedRows(t *testing.T) {
	parent := &ir.Definition{
		Schemas: []*ir.Schema{
			&ir.Schema{
				Name: "public",
				Tables: []*ir.Table{
					&ir.Table{
						Name:       "parent_table",
						PrimaryKey: []string{"pk"},
						Columns: []*ir.Column{
							{Name: "pk", Type: "int"},
							{Name: "col1", Type: "char(10)", Default: "yeahboy"},
						},
					},
				},
			},
		},
	}
	child := &ir.Definition{
		Schemas: []*ir.Schema{&ir.Schema{
			Name: "public",
			Tables: []*ir.Table{
				&ir.Table{
					Name:           "child_table",
					PrimaryKey:     []string{"pkchild"},
					InheritsSchema: "public",
					InheritsTable:  "parent_table",
					Columns: []*ir.Column{
						{Name: "pkchild", Type: "int"},
						{Name: "x", Type: "int"},
					},
					Rows: &ir.DataRows{
						Columns: []string{"pkchild", "col1"},
						Rows: []*ir.DataRow{
							&ir.DataRow{
								Columns: []*ir.DataCol{
									{Text: "99999999999999"},
									{Text: "techmology"},
								},
							},
						},
					},
				},
			},
		},
		},
	}
	parentAndChild := &ir.Definition{
		Schemas: []*ir.Schema{&ir.Schema{
			Name: "public",
			Tables: []*ir.Table{
				&ir.Table{
					Name:       "parent_table",
					PrimaryKey: []string{"pk"},
					Columns: []*ir.Column{
						{Name: "pk", Type: "int"},
						{Name: "col1", Type: "char(10)", Default: "yeahboy"},
					},
				},
				&ir.Table{
					Name:           "child_table",
					PrimaryKey:     []string{"pkchild"},
					InheritsSchema: "public",
					InheritsTable:  "parent_table",
					Columns: []*ir.Column{
						{Name: "pkchild", Type: "int"},
						{Name: "x", Type: "int"},
					},
					Rows: &ir.DataRows{
						Columns: []string{"pkchild", "col1"},
						Rows: []*ir.DataRow{
							&ir.DataRow{
								Columns: []*ir.DataCol{
									{Text: "99999999999999"},
									{Text: "techmology"},
								},
							},
						},
					},
				},
			},
		},
		},
	}
	grandchild := &ir.Definition{
		Schemas: []*ir.Schema{&ir.Schema{
			Name: "notpublic",
			Tables: []*ir.Table{
				&ir.Table{
					Name:           "grandchild_table",
					PrimaryKey:     []string{"pkgrandchild"},
					InheritsSchema: "public",
					InheritsTable:  "child_table",
					Columns: []*ir.Column{
						{Name: "pkgrandchild", Type: "int"},
						{Name: "y", Type: "int"},
					},
					Rows: &ir.DataRows{
						Columns: []string{"pkgrandchild", "col1"},
						Rows: []*ir.DataRow{
							&ir.DataRow{
								Columns: []*ir.DataCol{
									{Text: "99999999999999"},
									{Text: "techmology"},
								},
							},
						},
					},
				},
			},
		},
		},
	}

	// TODO(go,nth) v1 doesn't actually assert anything... should we? AFAICT we just verify it doesn't blow up
	// TODO(go,3) really we're testing schema.Merge functionality... I think?
	// TODO(go,nth) return errors instead of fataling
	_, err := lib.GlobalXmlParser.CompositeDoc(parent, child, "", -1, nil)
	assert.NoError(t, err)
	_, err = lib.GlobalXmlParser.CompositeDoc(parentAndChild, grandchild, "", -1, nil)
	assert.NoError(t, err)
}

func TestXmlParser_CompositeDoc_DuplicateFunctionValidation_SeparateDefs(t *testing.T) {
	// NOTE: v1 apparently broke at some point; it had been testing format::build_schema and
	// format::diff_doc_work, but the only place the duplicate function error is thrown is
	// inside xml_parser::xml_composite_children, so we're testing that instead here
	// TODO(go,3) we should probably test schema.Merge instead of XmlParser.CompositeDoc
	doc := &ir.Definition{
		Schemas: []*ir.Schema{
			&ir.Schema{
				Name: "someschema",
				Functions: []*ir.Function{
					&ir.Function{
						Name:        "lpad",
						Returns:     "text",
						CachePolicy: "IMMUTABLE",
						Parameters: []*ir.FunctionParameter{
							{Type: "text"},
							{Type: "int"},
							{Type: "text"},
						},
						Definitions: []*ir.FunctionDefinition{
							{Language: "sql", SqlFormat: ir.SqlFormatPgsql8, Text: "SELECT LPAD($1, $2, $3);"},
						},
					},
					// duplicates the above
					&ir.Function{
						Name:        "lpad",
						Returns:     "text",
						CachePolicy: "IMMUTABLE",
						Parameters: []*ir.FunctionParameter{
							{Type: "text"},
							{Type: "int"},
							{Type: "text"},
						},
						Definitions: []*ir.FunctionDefinition{
							{Language: "sql", SqlFormat: ir.SqlFormatPgsql8, Text: "SELECT LPAD($1, $2, $3);"},
						},
					},
					// has a different SqlFormat
					&ir.Function{
						Name:        "lpad",
						Returns:     "text",
						CachePolicy: "IMMUTABLE",
						Parameters: []*ir.FunctionParameter{
							{Type: "text"},
							{Type: "int"},
							{Type: "text"},
						},
						Definitions: []*ir.FunctionDefinition{
							{
								Language:  "sql",
								SqlFormat: ir.SqlFormatMssql10,
								Text: `BEGIN
									DECLARE @base_str_len int,
													@pad_len int,
													@padded_str VARCHAR(MAX)
									IF @str_len &lt; 1
									BEGIN
										RETURN ''
									END
									IF len(@pad_str) = 0 AND datalength(@pad_str) = 0
									BEGIN
										RETURN substring(@base_str, 1, @str_len)
									END
									SET @base_str_len = LEN(@base_str)
									SET @pad_len = ((@str_len-@base_str_len) / len(@pad_str)) + 1
									RETURN right(REPLICATE(@pad_str, @pad_len) + @base_str, @str_len)
								END`,
							},
						},
					},
				},
			},
		},
	}

	_, err := lib.GlobalXmlParser.CompositeDoc(nil, doc, "", -1, nil)
	if assert.Error(t, err) {
		assert.Contains(t, strings.ToLower(err.Error()), "found two functions in schema someschema with signature lpad(text, int, text) for sql format pgsql8")
		assert.NotContains(t, strings.ToLower(err.Error()), "for sql format mssql10")
	}
}

func TestXmlParser_CompositeDoc_DuplicateFunctionValidation_SharedDefs(t *testing.T) {
	doc := &ir.Definition{
		Schemas: []*ir.Schema{
			&ir.Schema{
				Name: "someschema",
				Functions: []*ir.Function{
					&ir.Function{
						Name:        "lpad",
						Returns:     "text",
						CachePolicy: "IMMUTABLE",
						Parameters: []*ir.FunctionParameter{
							{Type: "text"},
							{Type: "int"},
							{Type: "text"},
						},
						Definitions: []*ir.FunctionDefinition{
							{Language: "sql", SqlFormat: ir.SqlFormatPgsql8, Text: "SELECT LPAD($1, $2, $3);"},
							{Language: "sql", SqlFormat: ir.SqlFormatPgsql8, Text: "SELECT LPAD($1, $2, $3);"},
						},
					},
					// has a different SqlFormat
					&ir.Function{
						Name:        "lpad",
						Returns:     "text",
						CachePolicy: "IMMUTABLE",
						Parameters: []*ir.FunctionParameter{
							{Type: "text"},
							{Type: "int"},
							{Type: "text"},
						},
						Definitions: []*ir.FunctionDefinition{
							{
								Language:  "sql",
								SqlFormat: ir.SqlFormatMssql10,
								Text: `BEGIN
									DECLARE @base_str_len int,
													@pad_len int,
													@padded_str VARCHAR(MAX)
									IF @str_len &lt; 1
									BEGIN
										RETURN ''
									END
									IF len(@pad_str) = 0 AND datalength(@pad_str) = 0
									BEGIN
										RETURN substring(@base_str, 1, @str_len)
									END
									SET @base_str_len = LEN(@base_str)
									SET @pad_len = ((@str_len-@base_str_len) / len(@pad_str)) + 1
									RETURN right(REPLICATE(@pad_str, @pad_len) + @base_str, @str_len)
								END`,
							},
						},
					},
				},
			},
		},
	}

	_, err := lib.GlobalXmlParser.CompositeDoc(nil, doc, "", -1, nil)
	if assert.Error(t, err) {
		assert.Contains(t, strings.ToLower(err.Error()), "found two definitions for someschema.lpad(text, int, text) for sql format pgsql8")
		assert.NotContains(t, strings.ToLower(err.Error()), "for sql format mssql10")
	}
}
