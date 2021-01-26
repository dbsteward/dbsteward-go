package pgsql8

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/jackc/pgx/v4/pgxpool"
)

var GlobalDb *Db = NewDB()

type Db struct {
	conn *pgxpool.Pool
}

func NewDB() *Db {
	return &Db{}
}

func (self *Db) Connect(host string, port uint, name, user, pass string) {
	// TODO(go,3) sslmode?
	// TODO(go,3) just have the user pass the entire DSN
	// TODO(feat) support envvar password
	dsnNoPass := fmt.Sprintf("host=%s port=%d user=%s dbname=%s", host, port, user, name)
	dsn := dsnNoPass + fmt.Sprintf(" password=%s", pass)
	conn, err := pgxpool.Connect(context.Background(), dsn)
	lib.GlobalDBSteward.FatalIfError(err, "Could not connect to database %s", dsnNoPass)
	self.conn = conn
}

func (self *Db) Version() (int, error) {
	var v string
	err := self.QueryVal(&v, "SHOW server_version_num;")
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(v)
}

func (self *Db) Disconnect() {
	self.conn.Close()
}

func (self *Db) Query(query string, params ...interface{}) ([]map[string]string, error) {
	out := []map[string]string{}
	rows, err := self.conn.Query(context.TODO(), query, params...)
	if err != nil {
		return nil, err
	}

	fields := rows.FieldDescriptions()
	cols := make([]string, len(fields))
	vals := make([]sql.NullString, len(fields))
	dests := make([]interface{}, len(fields))
	for i, field := range fields {
		cols[i] = string(field.Name)
		dests[i] = &vals[i]
	}

	for rows.Next() {
		err := rows.Scan(dests...)
		if err != nil {
			return nil, err
		}

		m := map[string]string{}
		for i, col := range cols {
			m[col] = vals[i].String
		}

		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (self *Db) QueryVal(val interface{}, sql string, params ...interface{}) error {
	return self.conn.QueryRow(context.TODO(), sql, params...).Scan(val)
}

func (self *Db) QueryStringMap(sql string, params ...interface{}) (map[string]string, error) {
	recs, err := self.Query(sql, params...)
	if err != nil {
		return nil, err
	}
	if len(recs) == 0 {
		return nil, errors.New("No rows returned")
	}
	return recs[0], nil
}
