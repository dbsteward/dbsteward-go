package pgsql8

import (
	"context"
	"os"
	"testing"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format"
	"github.com/dbsteward/dbsteward/lib/ir"
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
	c := Initdb(t, "pg")
	if c == nil {
		t.SkipNow()
	}
	defer Teardowndb(t, c, "pg")
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
