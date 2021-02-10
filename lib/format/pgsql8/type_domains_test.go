package pgsql8_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

// NOTE these tests differ from v1 tests in that they don't test SQL building,
// only the correct sequence of DDL.
// Also the testDiff* tests were split to diff_types_domains_test.go

func TestType_Domain_GetCreationSql_NoDomainTypeThrows(t *testing.T) {
	dt := &model.DataType{
		Name: "my_domain",
		Kind: model.DataTypeKindDomain,
	}
	schema := &model.Schema{
		Name:  "domains",
		Types: []*model.DataType{dt},
	}

	_, err := pgsql8.NewDataType().GetCreationSql(schema, dt)
	if assert.Error(t, err, "GetCreationSql should return an error if there is no underlying domain type") {
		assert.Contains(t, err.Error(), "contains no domainType child")
	}
}

func TestType_Domain_GetCreationSql(t *testing.T) {
	dt := &model.DataType{
		Name: "my_domain",
		Kind: model.DataTypeKindDomain,
		DomainType: &model.DataTypeDomainType{
			BaseType: "int",
		},
	}
	schema := &model.Schema{
		Name:  "domains",
		Types: []*model.DataType{dt},
	}

	ddl, err := pgsql8.NewDataType().GetCreationSql(schema, dt)
	assert.NoError(t, err)
	assert.Equal(t, []output.ToSql{
		&sql.TypeDomainCreate{
			Type:        sql.TypeRef{"domains", "my_domain"},
			BaseType:    "int",
			Constraints: []sql.TypeDomainCreateConstraint{},
		},
	}, ddl)
}

func TestType_Domain_GetCreationSql_DefaultNotNull(t *testing.T) {
	dt := &model.DataType{
		Name: "my_domain",
		Kind: model.DataTypeKindDomain,
		DomainType: &model.DataTypeDomainType{
			BaseType: "int",
			Default:  "5",
			Nullable: false,
		},
	}
	schema := &model.Schema{
		Name:  "domains",
		Types: []*model.DataType{dt},
	}

	ddl, err := pgsql8.NewDataType().GetCreationSql(schema, dt)
	assert.NoError(t, err)
	assert.Equal(t, []output.ToSql{
		&sql.TypeDomainCreate{
			Type:        sql.TypeRef{"domains", "my_domain"},
			BaseType:    "int",
			Default:     "5",
			Nullable:    false,
			Constraints: []sql.TypeDomainCreateConstraint{},
		},
	}, ddl)
}

func TestType_Domain_GetCreationSql_Constraint(t *testing.T) {
	dt := &model.DataType{
		Name: "my_domain",
		Kind: model.DataTypeKindDomain,
		DomainType: &model.DataTypeDomainType{
			BaseType: "int",
		},
		DomainConstraints: []*model.DataTypeDomainConstraint{
			&model.DataTypeDomainConstraint{
				Name:  "gt_five",
				Check: "VALUE > 5",
			},
		},
	}
	schema := &model.Schema{
		Name:  "domains",
		Types: []*model.DataType{dt},
	}

	ddl, err := pgsql8.NewDataType().GetCreationSql(schema, dt)
	assert.NoError(t, err)
	assert.Equal(t, []output.ToSql{
		&sql.TypeDomainCreate{
			Type:     sql.TypeRef{"domains", "my_domain"},
			BaseType: "int",
			Constraints: []sql.TypeDomainCreateConstraint{
				sql.TypeDomainCreateConstraint{
					Name:  "gt_five",
					Check: "VALUE > 5",
				},
			},
		},
	}, ddl)
}

func TestType_Domain_GetCreationSql_MultipleConstraintsAndExplicitCheck(t *testing.T) {
	dt := &model.DataType{
		Name: "my_domain",
		Kind: model.DataTypeKindDomain,
		DomainType: &model.DataTypeDomainType{
			BaseType: "int",
		},
		DomainConstraints: []*model.DataTypeDomainConstraint{
			&model.DataTypeDomainConstraint{
				Name: "lt_ten",
				// should support all kinds of weird but equivalent spacing and casing
				Check: " CHEck ( VALUE < 10)",
			},
			&model.DataTypeDomainConstraint{
				Name:  "gt_five",
				Check: "VALUE > 5",
			},
		},
	}
	schema := &model.Schema{
		Name:  "domains",
		Types: []*model.DataType{dt},
	}

	ddl, err := pgsql8.NewDataType().GetCreationSql(schema, dt)
	assert.NoError(t, err)
	assert.Equal(t, []output.ToSql{
		&sql.TypeDomainCreate{
			Type:     sql.TypeRef{"domains", "my_domain"},
			BaseType: "int",
			Constraints: []sql.TypeDomainCreateConstraint{
				sql.TypeDomainCreateConstraint{
					Name:  "lt_ten",
					Check: "VALUE < 10",
				},
				sql.TypeDomainCreateConstraint{
					Name:  "gt_five",
					Check: "VALUE > 5",
				},
			},
		},
	}, ddl)
}

func TestType_Domain_GetCreationSql_QuotedDefault(t *testing.T) {
	// NOTE: In v1 this test attempted to verify that the default was turned
	// into a quoted value. In v2+, that behavior has been mostly pushed to
	// the sql layer. So, in effect this test now verifies that we _don't_ do
	// anything in the diff/build layer.
	dt := &model.DataType{
		Name: "my_domain",
		Kind: model.DataTypeKindDomain,
		DomainType: &model.DataTypeDomainType{
			BaseType: "varchar(20)",
			Default:  "abc",
		},
	}
	schema := &model.Schema{
		Name:  "domains",
		Types: []*model.DataType{dt},
	}

	ddl, err := pgsql8.NewDataType().GetCreationSql(schema, dt)
	assert.NoError(t, err)
	assert.Equal(t, []output.ToSql{
		&sql.TypeDomainCreate{
			Type:        sql.TypeRef{"domains", "my_domain"},
			BaseType:    "varchar(20)",
			Default:     "abc",
			Constraints: []sql.TypeDomainCreateConstraint{},
		},
	}, ddl)
}

func TestType_Domain_GetDropSql(t *testing.T) {
	dt := &model.DataType{
		Name: "my_domain",
		Kind: model.DataTypeKindDomain,
		DomainType: &model.DataTypeDomainType{
			BaseType: "varchar(20)",
			Default:  "abc",
		},
	}
	schema := &model.Schema{
		Name:  "domains",
		Types: []*model.DataType{dt},
	}

	ddl := pgsql8.NewDataType().GetDropSql(schema, dt)
	assert.Equal(t, []output.ToSql{
		&sql.TypeDomainDrop{
			Type: sql.TypeRef{"domains", "my_domain"},
		},
	}, ddl)
}
