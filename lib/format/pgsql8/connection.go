package pgsql8

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v4"

	"github.com/pkg/errors"
)

type connection interface {
	version() (VersionNum, error)
	disconnect()
	query(query string, params ...interface{}) (pgx.Rows, error)
	queryRow(query string, params ...interface{}) pgx.Row
	queryMap(query string, params ...interface{}) (StringMapList, error)
	queryVal(val interface{}, sql string, params ...interface{}) error
}

type connectionFactory interface {
	newConnection(host string, port uint, name, user, pass string) (connection, error)
}

type liveConnectionFactory struct{}

func (*liveConnectionFactory) newConnection(host string, port uint, name, user, pass string) (connection, error) {
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

type ConstantConnectionFactory struct {
	Connection connection
}

var _ connectionFactory = &ConstantConnectionFactory{}

func (self *ConstantConnectionFactory) newConnection(string, uint, string, string, string) (connection, error) {
	return self.Connection, nil
}

type NullConnection struct {
	connection
}

func (*NullConnection) disconnect() {}

type liveConnection struct {
	conn *pgx.Conn
}

type StringMap map[string]string
type StringMapList []StringMap

func (self *liveConnection) version() (VersionNum, error) {
	var v string // for reasons unknown, this won't scan to int, only string
	err := self.queryVal(&v, "SHOW server_version_num;")
	if err != nil {
		return 0, err
	}
	i, err := strconv.Atoi(v)
	return VersionNum(i), err
}

func (self *liveConnection) disconnect() {
	self.conn.Close(context.TODO())
}

func (self *liveConnection) query(query string, params ...interface{}) (pgx.Rows, error) {
	return self.conn.Query(context.TODO(), query, params...)
}
func (self *liveConnection) queryRow(query string, params ...interface{}) pgx.Row {
	return self.conn.QueryRow(context.TODO(), query, params...)
}

func (self *liveConnection) queryMap(query string, params ...interface{}) (StringMapList, error) {
	out := StringMapList{}
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

func (self *liveConnection) queryVal(val interface{}, sql string, params ...interface{}) error {
	return self.conn.QueryRow(context.TODO(), sql, params...).Scan(val)
}
