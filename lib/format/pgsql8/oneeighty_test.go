package pgsql8

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format"
	"github.com/dbsteward/dbsteward/lib/ir"
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
// * schema public description is not updated on create
// * Data types are not normalized nor standardized nor anything like that

func TestOneEighty(t *testing.T) {
	c := initdb(t)
	if c == nil {
		t.SkipNow()
	}
	defer teardowndb(t, c)
	role := os.Getenv("DB_USER")
	lib.GlobalDBSteward = lib.NewDBSteward(format.LookupMap{
		ir.SqlFormatPgsql8: GlobalLookup,
	})
	lib.GlobalDBSteward.SqlFormat = ir.SqlFormatPgsql8
	ops := NewOperations().(*Operations)
	statements, err := ops.CreateStatements(ir.FullFeatureSchema(role))
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
	assert.Equal(t, ir.FullFeatureSchema(role), *reflection, "reflection does not match original")
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
	_, err = conn.Exec(context.TODO(), fmt.Sprintf("CREATE ROLE %s", ir.AdditionalRole))
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
	_, err = conn.Exec(context.TODO(), fmt.Sprintf("DROP ROLE IF EXISTS %s", ir.AdditionalRole))
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
