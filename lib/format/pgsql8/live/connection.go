package live

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v4"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

//go:generate $ROOTDIR/run _mockgen Connection

type Connection interface {
	Version() (VersionNum, error)
	Disconnect()
	Query(query string, params ...interface{}) (pgx.Rows, error)
	QueryRow(query string, params ...interface{}) pgx.Row
	QueryMap(query string, params ...interface{}) (StringMapList, error)
	QueryVal(val interface{}, sql string, params ...interface{}) error
}

type ConnectionFactory interface {
	NewConnection(host string, port uint, name, user, pass string) (Connection, error)
}

type LiveConnectionFactory struct{}

func (*LiveConnectionFactory) NewConnection(host string, port uint, name, user, pass string) (Connection, error) {
	// TODO(go,3) sslmode?
	// TODO(go,3) just have the user pass the entire DSN
	// TODO(feat) support envvar password
	dsnNoPass := fmt.Sprintf("host=%s port=%d user=%s dbname=%s", host, port, user, name)
	dsn := dsnNoPass + fmt.Sprintf(" password=%s", pass)
	conn, err := pgxpool.Connect(context.Background(), dsn)
	if err != nil {
		return nil, errors.Wrap(err, "Could not connect to postgres database")
	}

	return &LiveConnection{conn}, nil
}

type ConstantConnectionFactory struct {
	Connection Connection
}

var _ ConnectionFactory = &ConstantConnectionFactory{}

func (self *ConstantConnectionFactory) NewConnection(string, uint, string, string, string) (Connection, error) {
	return self.Connection, nil
}

type NullConnection struct {
	Connection
}

func (*NullConnection) Disconnect() {}

type LiveConnection struct {
	conn *pgxpool.Pool
}

type StringMap map[string]string
type StringMapList []StringMap

func (self *LiveConnection) Version() (VersionNum, error) {
	var v string // for reasons unknown, this won't scan to int, only string
	err := self.QueryVal(&v, "SHOW server_version_num;")
	if err != nil {
		return 0, err
	}
	i, err := strconv.Atoi(v)
	return VersionNum(i), err
}

func (self *LiveConnection) Disconnect() {
	self.conn.Close()
}

func (self *LiveConnection) Query(query string, params ...interface{}) (pgx.Rows, error) {
	return self.conn.Query(context.TODO(), query, params...)
}
func (self *LiveConnection) QueryRow(query string, params ...interface{}) pgx.Row {
	return self.conn.QueryRow(context.TODO(), query, params...)
}

func (self *LiveConnection) QueryMap(query string, params ...interface{}) (StringMapList, error) {
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

func (self *LiveConnection) QueryVal(val interface{}, sql string, params ...interface{}) error {
	return self.conn.QueryRow(context.TODO(), sql, params...).Scan(val)
}
