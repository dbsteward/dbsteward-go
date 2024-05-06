package ir

import "github.com/dbsteward/dbsteward/lib/util"

// Contains data and functions for testing against IR

const AdditionalRole = "additional_role"

// FullFeatureSchema returns a schema that should implement
// all the features that need to be supported. This is
// implemented as a function to ensure that tests can't
// change the value and corrupt other tests.
// @TODO: this does not yet contain all the features
func FullFeatureSchema(role string) Definition {
	return Definition{
		Database: &Database{
			SqlFormat: SqlFormatPgsql8,
			Roles: &RoleAssignment{
				Owner:       role,
				Application: role,
				CustomRoles: []string{AdditionalRole},
			},
		},
		Schemas: []*Schema{
			{
				Name:        "empty_schema",
				Description: "test empty schema properly processed",
				Owner:       role,
			},
			{
				Name:        "serial_schema",
				Description: "test serials handled appropriately",
				Owner:       role,
				Tables: []*Table{
					{
						Name:           "t1",
						Owner:          role,
						PrimaryKeyName: "t1_pkey",
						PrimaryKey:     []string{"id"},
						Columns: []*Column{
							{Name: "id", Type: "serial"},
							{
								Name:  "c2",
								Type:  "integer",
								Check: "c2 > 0",
							},
						},
					},
				},
				Sequences: []*Sequence{{
					Name:          "t1_id_seq",
					Owner:         "postgres",
					OwnedBySchema: "serial_schema",
					OwnedByTable:  "t1",
					OwnedByColumn: "id",
					Cache:         util.Some(1),
					Start:         util.Some(1),
					Min:           util.Some(1),
					Max:           util.Some(2147483647),
					Increment:     util.Some(1),
				}},
			},
			{
				Name:        "sequence_schema",
				Description: "test schema with a single sequence",
				Owner:       role,
				Sequences: []*Sequence{{
					Name:        "test_seq",
					Owner:       role,
					Description: "Test sequence detached from serial",
					Cache:       util.Some(3),
					Start:       util.Some(7),
					Min:         util.Some(5),
					Max:         util.Some(9876543),
					Increment:   util.Some(2),
				}},
			},
			{
				Name:        "other_function_schema",
				Description: "used as part of column_default_function_schema to test cross-schema default func references",
				Owner:       role,
				Functions: []*Function{
					{
						Name:        "test",
						Owner:       role,
						Description: "Test Function",
						CachePolicy: "VOLATILE",
						Returns:     "integer",
						Definitions: []*FunctionDefinition{{
							SqlFormat: SqlFormatPgsql8,
							Language:  "sql",
							Text:      "SELECT 1",
						}},
					},
				},
			},
			{
				Name:        "column_default_function_schema",
				Description: "test column default is a function works properly",
				Owner:       role,
				Functions: []*Function{
					{
						Name:        "test",
						Owner:       role,
						Description: "Test Function 1",
						CachePolicy: "VOLATILE",
						Returns:     "integer",
						Definitions: []*FunctionDefinition{{
							SqlFormat: SqlFormatPgsql8,
							Language:  "sql",
							Text:      "SELECT 1",
						}},
					},
				},
				Tables: []*Table{
					{
						Name:           "rate_group",
						Owner:          role,
						PrimaryKeyName: "rate_group_pkey",
						PrimaryKey:     []string{"rate_group_id"},
						Columns: []*Column{
							{Name: "rate_group_id", Type: "integer", Nullable: false, Default: "column_default_function_schema.test()"},
							{Name: "rate_group_name", Type: "character varying(100)", Nullable: true},
							{Name: "rate_group_enabled", Type: "boolean", Nullable: false, Default: "true"},
						},
					},
					{
						Name:           "rate_group_n",
						Owner:          role,
						PrimaryKeyName: "rate_group_n_pkey",
						PrimaryKey:     []string{"rate_group_id"},
						Columns: []*Column{
							{Name: "rate_group_id", Type: "integer", Nullable: false, Default: "other_function_schema.test()"},
							{Name: "rate_group_name", Type: "character varying(100)", Nullable: true},
							{Name: "rate_group_enabled", Type: "boolean", Nullable: false, Default: "true"},
						},
					},
				},
			},
			{
				Name:        "identifier_schema",
				Description: "test identifiers are quoted appropriately",
				Owner:       role,
				Tables: []*Table{
					{
						Name:           "t1",
						Owner:          role,
						PrimaryKeyName: "t1_pkey",
						PrimaryKey:     []string{"id"},
						Columns: []*Column{
							{Name: "id", Type: "integer"},
							{Name: "quoted\"c2\"", Type: "integer"},
							{Name: "multi word", Type: "integer"},
							{Name: "0startswithnumber", Type: "integer"},
							{Name: "select", Type: "integer"},
						},
					},
				},
			},
			{
				Name:        "public",
				Description: "standard public schema",
				Owner:       role,
				Tables: []*Table{
					{
						Name:           "t1",
						Description:    "Test table 1",
						Owner:          role,
						PrimaryKey:     []string{"id"},
						PrimaryKeyName: "t1_pkey",
						Columns: []*Column{
							{
								Name: "id",
								Type: "integer",
							},
							{
								Name: "name",
								Type: "text",
							},
						},
						Indexes: []*Index{
							{
								Using: IndexTypeBtree,
								Name:  "test_standalone_index",
								Dimensions: []*IndexDim{
									{
										Name:  "test_standalone_index_1",
										Value: "id",
									},
									{
										Name:  "test_standalone_index_2",
										Value: "name",
									},
								},
								Conditions: []*IndexCond{{
									SqlFormat: SqlFormatPgsql8,
									Condition: "name IS NOT NULL",
								}},
							},
							{
								Using: IndexTypeHash,
								Name:  "test_hash_index",
								Dimensions: []*IndexDim{
									{
										Name:  "test_hash_index_1",
										Value: "id",
									},
								},
							},
						},
					},
					{
						Name:           "t2",
						Owner:          role,
						PrimaryKey:     []string{"id"},
						PrimaryKeyName: "t2_pkey",
						Columns: []*Column{
							{
								Name: "id",
								Type: "bigint",
							},
							{
								Name:        "unique_col",
								Type:        "text",
								Description: "Ensure unqiue constraint behaves",
								Nullable:    true,
								Unique:      true,
							},
							{
								Name:            "nameref",
								ForeignKeyName:  "name_fk_4602",
								ForeignSchema:   "public",
								ForeignTable:    "t1",
								ForeignColumn:   "id",
								ForeignOnUpdate: ForeignKeyActionNoAction,
								ForeignOnDelete: ForeignKeyActionNoAction,
							},
						},
					},
				},
				Functions: []*Function{
					{
						Name:        "func1",
						Owner:       role,
						Description: "Text Function 1",
						CachePolicy: "VOLATILE",
						Returns:     "integer",
						Parameters: []*FunctionParameter{{
							Name:      "f1p1",
							Type:      "integer",
							Direction: FuncParamDirIn,
						}},
						Definitions: []*FunctionDefinition{{
							SqlFormat: SqlFormatPgsql8,
							Language:  "sql",
							Text:      "SELECT $1",
						}},
					},
				},
				Views: []*View{{
					Name:        "view0",
					Description: "test view 0",
					Owner:       role,
					Queries: []*ViewQuery{{
						SqlFormat: SqlFormatPgsql8,
						Text:      " SELECT id\n   FROM t2;",
					}},
				}},
				Grants: []*Grant{
					{
						Roles:       []string{AdditionalRole},
						Permissions: []string{"USAGE"},
					},
					{
						Roles:       []string{role},
						Permissions: []string{"CREATE"},
					},
					{
						Roles:       []string{role},
						Permissions: []string{"USAGE"},
					},
				},
				Types:     nil,
				Sequences: nil,
				Triggers:  nil,
			},
		},
	}
}
