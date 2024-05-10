package main

import (
	"context"
	_ "embed"
	"log/slog"
	"strings"
	"testing"

	"github.com/dbsteward/dbsteward/lib/encoding/xml"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8"
)

//go:embed example/someapp_v1.xml
var v1 string

//go:embed example/someapp_v2.xml
var v2 string

// To run:
// DB_HOST=localhost DB_USER=postgres DB_SUPERUSER=postgres DB_NAME=test DB_PORT=5432 go test ./...

// This test uses the definitions in the examples table to
// create a database and then create a set of upgrade commands
// and ensure those commands work. It is limited: see the comments
// at the end of the test for explanation.
func TestXMLPostgresIngegration(t *testing.T) {
	c := pgsql8.Initdb(t, "tl")
	if c == nil {
		t.SkipNow()
	}
	defer pgsql8.Teardowndb(t, c, "tl")
	def1, err := xml.ReadDef(strings.NewReader(v1))
	if err != nil {
		t.Fatal(err)
	}
	err = pgsql8.CreateRoleIfNotExists(c, def1.Database.Roles.Application)
	if err != nil {
		t.Fatal(err)
	}
	err = pgsql8.CreateRoleIfNotExists(c, def1.Database.Roles.Owner)
	if err != nil {
		t.Fatal(err)
	}
	err = pgsql8.CreateRoleIfNotExists(c, def1.Database.Roles.ReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	err = pgsql8.CreateRoleIfNotExists(c, def1.Database.Roles.Replication)
	if err != nil {
		t.Fatal(err)
	}
	ops := pgsql8.NewOperations(pgsql8.DefaultConfig).(*pgsql8.Operations)
	statements, err := ops.CreateStatements(*def1)
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
	def2, err := xml.ReadDef(strings.NewReader(v2))
	if err != nil {
		t.Fatal(err)
	}
	ops = pgsql8.NewOperations(pgsql8.DefaultConfig).(*pgsql8.Operations)
	statements, err = ops.Upgrade(slog.Default(), def1, def2)
	if err != nil {
		t.Fatal(err)
	}
	tx, err = c.Begin(context.TODO())
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
	ops = pgsql8.NewOperations(pgsql8.DefaultConfig).(*pgsql8.Operations)
	_, err = ops.ExtractSchemaConn(context.TODO(), c)
	if err != nil {
		t.Fatal(err)
	}
	// It's impractical to verify that the extraction is
	// correct without massively rewriting the XML. Due to
	// differences in object ordering and other things that
	// produce unequal but functionally equivalent code.
	// It's probably best to do that level of precision
	// testing at a more unit level.
}
