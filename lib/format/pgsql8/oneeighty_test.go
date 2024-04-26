package pgsql8

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/hexops/gotextdiff"
	"github.com/hexops/gotextdiff/myers"
	"github.com/hexops/gotextdiff/span"
	"github.com/jackc/pgx/v4"
)

// one eighty test uses and IR to build a database
// then extracts it and ensures it results in the
// same IR

// To run:
// DB_HOST=localhost DB_USER=postgres DB_SUPERUSER=postgres DB_NAME=test DB_PORT=5432 go test ./...

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
				Owner: role,
			},
		},
		Schemas: []*ir.Schema{
			{
				Name:  "public",
				Owner: role,
				Tables: []*ir.Table{
					{
						Name:           "t1",
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
				},
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
	if !reflect.DeepEqual(def, *reflection) {
		mExpect, err := json.MarshalIndent(def, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		mActual, err := json.MarshalIndent(*reflection, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		edits := myers.ComputeEdits(span.URI(""), string(mExpect), string(mActual))
		t.Fatal(fmt.Sprint(gotextdiff.ToUnified("Expected", "Actual", string(mExpect), edits)))
	}
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
	_, _ = conn.Exec(context.TODO(), fmt.Sprintf("DROP DATABASE %s IF EXISTS", os.Getenv("DB_NAME")))
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
