package pgsql8

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
)

// one eighty test uses and IR to build a database
// then extracts it and ensures it results in the
// same IR

// To run:
// DB_HOST=localhost DB_USER=postgres DB_SUPERUSER=postgres DB_NAME=test DB_PORT=5432 go test ./...

// TODO list: Things that don't work yet but are feature improvements
// * column UNIQUE setting is lost and turns into an index
// * schema public description is not updated on create
// * Data types are not normalized nor standardized nor anything like that
// * column check constraints are lost and converted to table constraints

const aRole = "additional_role"

func TestOneEighty(t *testing.T) {
	c := initdb(t)
	if c == nil {
		t.SkipNow()
	}
	defer teardowndb(t, c)
	role := os.Getenv("DB_USER")
	def := ir.Definition{
		Database: &ir.Database{
			SqlFormat: ir.SqlFormatPgsql8,
			Roles: &ir.RoleAssignment{
				Owner:       role,
				Application: role,
				CustomRoles: []string{aRole},
			},
		},
		Schemas: []*ir.Schema{
			{
				Name:        "empty_schema",
				Description: "test empty schema properly processed",
				Owner:       role,
			},
			{
				Name:        "serial_schema",
				Description: "test serials handled appropriately",
				Owner:       role,
				Tables: []*ir.Table{
					{
						Name:           "t1",
						Owner:          role,
						PrimaryKeyName: "t1_pkey",
						PrimaryKey:     []string{"id"},
						Columns: []*ir.Column{
							{Name: "id", Type: "serial"},
						},
					},
				},
				Sequences: []*ir.Sequence{{
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
				Sequences: []*ir.Sequence{{
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
				Functions: []*ir.Function{
					{
						Name:        "test",
						Owner:       role,
						Description: "Test Function",
						CachePolicy: "VOLATILE",
						Returns:     "integer",
						Definitions: []*ir.FunctionDefinition{{
							SqlFormat: ir.SqlFormatPgsql8,
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
				Functions: []*ir.Function{
					{
						Name:        "test",
						Owner:       role,
						Description: "Test Function 1",
						CachePolicy: "VOLATILE",
						Returns:     "integer",
						Definitions: []*ir.FunctionDefinition{{
							SqlFormat: ir.SqlFormatPgsql8,
							Language:  "sql",
							Text:      "SELECT 1",
						}},
					},
				},
				Tables: []*ir.Table{
					{
						Name:           "rate_group",
						Owner:          role,
						PrimaryKeyName: "rate_group_pkey",
						PrimaryKey:     []string{"rate_group_id"},
						Columns: []*ir.Column{
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
						Columns: []*ir.Column{
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
				Tables: []*ir.Table{
					{
						Name:           "t1",
						Owner:          role,
						PrimaryKeyName: "t1_pkey",
						PrimaryKey:     []string{"id"},
						Columns: []*ir.Column{
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
				Tables: []*ir.Table{
					{
						Name:           "t1",
						Description:    "Test table 1",
						Owner:          role,
						PrimaryKey:     []string{"id"},
						PrimaryKeyName: "t1_pkey",
						Columns: []*ir.Column{
							{
								Name: "id",
								Type: "integer",
							},
							{
								Name: "name",
								Type: "text",
							},
						},
					},
					{
						Name:           "t2",
						Owner:          role,
						PrimaryKey:     []string{"id"},
						PrimaryKeyName: "t2_pkey",
						Columns: []*ir.Column{
							{
								Name: "id",
								Type: "bigint",
							},
							{
								Name:     "description",
								Type:     "text",
								Nullable: true,
							},
							{
								Name:            "nameref",
								ForeignKeyName:  "name_fk_4602",
								ForeignSchema:   "public",
								ForeignTable:    "t1",
								ForeignColumn:   "id",
								ForeignOnUpdate: ir.ForeignKeyActionNoAction,
								ForeignOnDelete: ir.ForeignKeyActionNoAction,
							},
						},
					},
				},
				Functions: []*ir.Function{
					{
						Name:        "func1",
						Owner:       role,
						Description: "Text Function 1",
						CachePolicy: "VOLATILE",
						Returns:     "integer",
						Parameters: []*ir.FunctionParameter{{
							Name:      "f1p1",
							Type:      "integer",
							Direction: ir.FuncParamDirIn,
						}},
						Definitions: []*ir.FunctionDefinition{{
							SqlFormat: ir.SqlFormatPgsql8,
							Language:  "sql",
							Text:      "SELECT $1",
						}},
					},
				},
				Views: []*ir.View{{
					Name:        "view0",
					Description: "test view 0",
					Owner:       role,
					Queries: []*ir.ViewQuery{{
						SqlFormat: ir.SqlFormatPgsql8,
						Text:      " SELECT id\n   FROM t2;",
					}},
				}},
				Grants: []*ir.Grant{
					{
						Roles:       []string{aRole},
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
	lib.GlobalDBSteward = lib.NewDBSteward(format.LookupMap{
		ir.SqlFormatPgsql8: GlobalLookup,
	})
	lib.GlobalDBSteward.SqlFormat = ir.SqlFormatPgsql8
	ops := NewOperations()
	statements, err := ops.CreateStatements(def)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := c.Begin(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.TODO())
	for _, s := range statements {
		t.Log(s.Statement)
		_, err = tx.Exec(context.TODO(), s.Statement)
		if err != nil {
			t.Fatal(err.Error())
		}
	}
	err = tx.Commit(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	reflection, err := ops.ExtractSchemaConn(context.TODO(), c)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, def, *reflection, "reflection does not match original")
}

func initdb(t *testing.T) *pgx.Conn {
	if os.Getenv("DB_NAME") == "" {
		return nil
	}
	conn, err := pgx.Connect(context.TODO(), adminDSNFromEnv())
	if err != nil {
		t.Fatal(err)
		return nil
	}
	defer conn.Close(context.TODO())
	_, err = conn.Exec(context.TODO(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", os.Getenv("DB_NAME")))
	if err != nil {
		t.Fatal(err)
		return nil
	}
	_, err = conn.Exec(context.TODO(), fmt.Sprintf("CREATE DATABASE %s", os.Getenv("DB_NAME")))
	if err != nil {
		t.Fatal(err)
		return nil
	}
	_, err = conn.Exec(context.TODO(), fmt.Sprintf("CREATE ROLE %s", aRole))
	if err != nil {
		if (err.(*pgconn.PgError)).Code != "42710" { // Role exists
			t.Fatal(err)
			return nil
		}
	}
	err = conn.Close(context.TODO())
	if err != nil {
		t.Fatal(err)
		return nil
	}
	conn, err = pgx.Connect(context.TODO(), userDSNFromEnv())
	if err != nil {
		t.Fatal(err)
		return nil
	}
	return conn
}

func teardowndb(t *testing.T, c *pgx.Conn) {
	err := c.Close(context.TODO())
	if err != nil {
		t.Fatal(err)
		return
	}
	conn, err := pgx.Connect(context.TODO(), adminDSNFromEnv())
	if err != nil {
		t.Fatal(err)
		return
	}
	defer conn.Close(context.TODO())
	_, err = conn.Exec(context.TODO(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", os.Getenv("DB_NAME")))
	if err != nil {
		t.Log(err)
	}
	_, err = conn.Exec(context.TODO(), fmt.Sprintf("DROP ROLE IF EXISTS %s", aRole))
	if err != nil {
		t.Log(err)
	}
}

func adminDSNFromEnv() string {
	host := os.Getenv("DB_HOST")
	user := os.Getenv("DB_SUPERUSER")
	password := os.Getenv("DB_PASSWORD")
	dbName := "postgres"
	port := os.Getenv("DB_PORT")
	return fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%s",
		host,
		user,
		password,
		dbName,
		port,
	)
}

func userDSNFromEnv() string {
	host := os.Getenv("DB_HOST")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")
	port := os.Getenv("DB_PORT")
	cs := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%s",
		host,
		user,
		password,
		dbName,
		port,
	)
	return cs
}
