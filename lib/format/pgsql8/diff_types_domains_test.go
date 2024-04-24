package pgsql8

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/pgtestutil"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

func TestDiffTypes_Domain_BaseType(t *testing.T) {
	oldSchema := &ir.Schema{
		Name: "domains",
		Types: []*ir.DataType{
			&ir.DataType{
				Name: "my_domain",
				Kind: ir.DataTypeKindDomain,
				DomainType: &ir.DataTypeDomainType{
					BaseType: "int",
				},
			},
		},
	}

	newSchema := &ir.Schema{
		Name: "domains",
		Types: []*ir.DataType{
			&ir.DataType{
				Name: "my_domain",
				Kind: ir.DataTypeKindDomain,
				DomainType: &ir.DataTypeDomainType{
					BaseType: "varchar(20)",
				},
			},
		},
	}

	ddl := diffTypes(oldSchema, newSchema)
	assert.Equal(t, []output.ToSql{
		&sql.TypeDomainDrop{sql.TypeRef{"domains", "my_domain"}},
		&sql.TypeDomainCreate{
			Type:        sql.TypeRef{"domains", "my_domain"},
			BaseType:    "varchar(20)",
			Constraints: []sql.TypeDomainCreateConstraint{},
		},
	}, ddl)
}

func TestDiffTypes_Domain_ChangeDefault(t *testing.T) {
	oldSchema := &ir.Schema{
		Name: "domains",
		Types: []*ir.DataType{
			&ir.DataType{
				Name: "my_domain",
				Kind: ir.DataTypeKindDomain,
				DomainType: &ir.DataTypeDomainType{
					BaseType: "int",
					Default:  "5",
				},
			},
		},
	}

	newSchema := &ir.Schema{
		Name: "domains",
		Types: []*ir.DataType{
			&ir.DataType{
				Name: "my_domain",
				Kind: ir.DataTypeKindDomain,
				DomainType: &ir.DataTypeDomainType{
					BaseType: "int",
					Default:  "10",
				},
			},
		},
	}

	ddl := diffTypes(oldSchema, newSchema)
	assert.Equal(t, []output.ToSql{
		&sql.TypeDomainAlterSetDefault{
			Type:  sql.TypeRef{"domains", "my_domain"},
			Value: &sql.TypedValue{"int", "10", false},
		},
	}, ddl)
}

func TestDiffTypes_Domain_DropDefault(t *testing.T) {
	oldSchema := &ir.Schema{
		Name: "domains",
		Types: []*ir.DataType{
			&ir.DataType{
				Name: "my_domain",
				Kind: ir.DataTypeKindDomain,
				DomainType: &ir.DataTypeDomainType{
					BaseType: "int",
					Default:  "5",
				},
			},
		},
	}

	newSchema := &ir.Schema{
		Name: "domains",
		Types: []*ir.DataType{
			&ir.DataType{
				Name: "my_domain",
				Kind: ir.DataTypeKindDomain,
				DomainType: &ir.DataTypeDomainType{
					BaseType: "int",
				},
			},
		},
	}

	ddl := diffTypes(oldSchema, newSchema)
	assert.Equal(t, []output.ToSql{
		&sql.TypeDomainAlterDropDefault{sql.TypeRef{"domains", "my_domain"}},
	}, ddl)
}

func TestDiffTypes_Domain_MakeNull(t *testing.T) {
	oldSchema := &ir.Schema{
		Name: "domains",
		Types: []*ir.DataType{
			&ir.DataType{
				Name: "my_domain",
				Kind: ir.DataTypeKindDomain,
				DomainType: &ir.DataTypeDomainType{
					BaseType: "int",
					Nullable: false,
				},
			},
		},
	}

	newSchema := &ir.Schema{
		Name: "domains",
		Types: []*ir.DataType{
			&ir.DataType{
				Name: "my_domain",
				Kind: ir.DataTypeKindDomain,
				DomainType: &ir.DataTypeDomainType{
					BaseType: "int",
					Nullable: true,
				},
			},
		},
	}

	ddl := diffTypes(oldSchema, newSchema)
	assert.Equal(t, []output.ToSql{
		&sql.TypeDomainAlterSetNullable{sql.TypeRef{"domains", "my_domain"}, true},
	}, ddl)
}

func TestDiffTypes_Domain_MakeNotNull(t *testing.T) {
	oldSchema := &ir.Schema{
		Name: "domains",
		Types: []*ir.DataType{
			&ir.DataType{
				Name: "my_domain",
				Kind: ir.DataTypeKindDomain,
				DomainType: &ir.DataTypeDomainType{
					BaseType: "int",
					Nullable: true,
				},
			},
		},
	}

	newSchema := &ir.Schema{
		Name: "domains",
		Types: []*ir.DataType{
			&ir.DataType{
				Name: "my_domain",
				Kind: ir.DataTypeKindDomain,
				DomainType: &ir.DataTypeDomainType{
					BaseType: "int",
					Nullable: false,
				},
			},
		},
	}

	ddl := diffTypes(oldSchema, newSchema)
	assert.Equal(t, []output.ToSql{
		&sql.TypeDomainAlterSetNullable{sql.TypeRef{"domains", "my_domain"}, false},
	}, ddl)
}

func TestDiffTypes_Domain_AddDropChangeConstraints(t *testing.T) {
	oldSchema := &ir.Schema{
		Name: "domains",
		Types: []*ir.DataType{
			&ir.DataType{
				Name: "my_domain",
				Kind: ir.DataTypeKindDomain,
				DomainType: &ir.DataTypeDomainType{
					BaseType: "int",
				},
				DomainConstraints: []*ir.DataTypeDomainConstraint{
					{"gt5", "VALUE > 5"},
					{"lt10", "VALUE < 10"},
					{"eq7", "VALUE = 7"},
				},
			},
		},
	}

	newSchema := &ir.Schema{
		Name: "domains",
		Types: []*ir.DataType{
			&ir.DataType{
				Name: "my_domain",
				Kind: ir.DataTypeKindDomain,
				DomainType: &ir.DataTypeDomainType{
					BaseType: "int",
				},
				DomainConstraints: []*ir.DataTypeDomainConstraint{
					{"gt5", "CHECK(VALUE > 5)"},
					{"gt4", "VALUE > 4"},
					{"eq7", "VALUE = 2"},
				},
			},
		},
	}

	ddl := diffTypes(oldSchema, newSchema)
	ref := sql.TypeRef{"domains", "my_domain"}
	assert.Equal(t, []output.ToSql{
		&sql.TypeDomainAlterAddConstraint{ref, "gt4", sql.RawSql("VALUE > 4")},
		&sql.TypeDomainAlterDropConstraint{ref, "eq7"},
		&sql.TypeDomainAlterAddConstraint{ref, "eq7", sql.RawSql("VALUE = 2")},
		&sql.TypeDomainAlterDropConstraint{ref, "lt10"},
	}, ddl)
}

func TestDiffTypes_Domain_DependentColumn(t *testing.T) {
	oldSchema := &ir.Schema{
		Name: "domains",
		Tables: []*ir.Table{
			&ir.Table{
				Name:       "some_table",
				PrimaryKey: []string{"col1"},
				Columns: []*ir.Column{
					{Name: "col1", Type: "int", Nullable: false},
					{Name: "mycol", Type: "my_domain"},
				},
			},
		},
		Types: []*ir.DataType{
			&ir.DataType{
				Name: "my_domain",
				Kind: ir.DataTypeKindDomain,
				DomainType: &ir.DataTypeDomainType{
					BaseType: "int",
				},
				DomainConstraints: []*ir.DataTypeDomainConstraint{
					{"gt5", "VALUE > 5"},
				},
			},
		},
	}

	newSchema := &ir.Schema{
		Name: "domains",
		Tables: []*ir.Table{
			&ir.Table{
				Name:       "some_table",
				PrimaryKey: []string{"col1"},
				Columns: []*ir.Column{
					{Name: "col1", Type: "int", Nullable: false},
					{Name: "mycol", Type: "domains.my_domain"},
				},
			},
		},
		Types: []*ir.DataType{
			&ir.DataType{
				Name: "my_domain",
				Kind: ir.DataTypeKindDomain,
				DomainType: &ir.DataTypeDomainType{
					BaseType: "int",
				},
				DomainConstraints: []*ir.DataTypeDomainConstraint{
					{"gt5", "VALUE > 3"},
				},
			},
		},
	}

	ddl := diffTypes(oldSchema, newSchema)
	ref := sql.TypeRef{"domains", "my_domain"}
	assert.Equal(t, []output.ToSql{
		sql.NewTableAlter(
			sql.TableRef{"domains", "some_table"},
			&sql.TableAlterPartColumnChangeType{"mycol", sql.ParseTypeRef("int"), nil},
		),
		&sql.TypeDomainAlterDropConstraint{ref, "gt5"},
		&sql.TypeDomainAlterAddConstraint{ref, "gt5", sql.RawSql("VALUE > 3")},
		sql.NewTableAlter(
			sql.TableRef{"domains", "some_table"},
			&sql.TableAlterPartColumnChangeTypeUsingCast{"mycol", ref},
		),
	}, ddl)
}

func diffTypes(oldSchema, newSchema *ir.Schema) []output.ToSql {
	oldDoc := &ir.Definition{
		Schemas: []*ir.Schema{oldSchema},
	}
	newDoc := &ir.Definition{
		Schemas: []*ir.Schema{newSchema},
	}
	setOldNewDocs(oldDoc, newDoc)
	ofs := &pgtestutil.RecordingOfs{
		StripComments: true,
	}
	GlobalDiffTypes.DiffTypes(ofs, oldSchema, newSchema)
	return ofs.Sql
}
