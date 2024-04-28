package pgsql8

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v4"

	"github.com/pkg/errors"
)

func newConnection(host string, port uint, name, user, pass string) (*liveConnection, error) {
	// TODO(go,3) sslmode?
	// TODO(go,3) just have the user pass the entire DSN
	// TODO(feat) support envvar password
	dsnNoPass := fmt.Sprintf("host=%s port=%d user=%s dbname=%s", host, port, user, name)
	dsn := dsnNoPass + fmt.Sprintf(" password=%s", pass)
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		return nil, errors.Wrap(err, "Could not connect to postgres database")
	}

	return &liveConnection{conn}, nil
}

type liveConnection struct {
	conn *pgx.Conn
}

type StringMap map[string]string
type StringMapList []StringMap

func (lconn *liveConnection) version() (VersionNum, error) {
	var v string // for reasons unknown, this won't scan to int, only string
	err := lconn.queryVal(&v, "SHOW server_version_num;")
	if err != nil {
		return 0, err
	}
	i, err := strconv.Atoi(v)
	return VersionNum(i), err
}

func (lconn *liveConnection) disconnect() {
	lconn.conn.Close(context.TODO())
}

func (lconn *liveConnection) query(query string, params ...interface{}) (pgx.Rows, error) {
	return lconn.conn.Query(context.TODO(), query, params...)
}
func (lconn *liveConnection) queryRow(query string, params ...interface{}) pgx.Row {
	return lconn.conn.QueryRow(context.TODO(), query, params...)
}

func (lconn *liveConnection) queryMap(query string, params ...interface{}) (StringMapList, error) {
	out := StringMapList{}
	rows, err := lconn.conn.Query(context.TODO(), query, params...)
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

		m := StringMap{}
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

func (lconn *liveConnection) queryVal(val interface{}, sql string, params ...interface{}) error {
	return lconn.conn.QueryRow(context.TODO(), sql, params...).Scan(val)
}
