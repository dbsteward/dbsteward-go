package pgsql8

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

func Initdb(t *testing.T, dbSuffix string) *pgx.Conn {
	if os.Getenv("DB_NAME") == "" {
		return nil
	}
	conn, err := pgx.Connect(context.TODO(), adminDSNFromEnv())
	if err != nil {
		t.Fatal(err)
		return nil
	}
	defer conn.Close(context.TODO())
	_, err = conn.Exec(context.TODO(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", os.Getenv("DB_NAME")+dbSuffix))
	if err != nil {
		t.Fatal(err)
		return nil
	}
	_, err = conn.Exec(context.TODO(), fmt.Sprintf("CREATE DATABASE %s", os.Getenv("DB_NAME")+dbSuffix))
	if err != nil {
		t.Fatal(err)
		return nil
	}
	err = CreateRoleIfNotExists(conn, ir.AdditionalRole)
	if err != nil {
		t.Fatal(err)
		return nil
	}
	err = conn.Close(context.TODO())
	if err != nil {
		t.Fatal(err)
		return nil
	}
	conn, err = pgx.Connect(context.TODO(), userDSNFromEnv(dbSuffix))
	if err != nil {
		t.Fatal(err)
		return nil
	}
	return conn
}

func CreateRoleIfNotExists(conn *pgx.Conn, name string) error {
	_, err := conn.Exec(context.TODO(), fmt.Sprintf("CREATE ROLE %s", name))
	if err != nil {
		code := (err.(*pgconn.PgError)).Code
		if code != "42710" && code != "23505" { // Role exists
			return err
		}
	}
	return nil
}

func Teardowndb(t *testing.T, c *pgx.Conn, dbSuffix string) {
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
	_, err = conn.Exec(context.TODO(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", os.Getenv("DB_NAME")+dbSuffix))
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

func userDSNFromEnv(suffix string) string {
	host := os.Getenv("DB_HOST")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME") + suffix
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
