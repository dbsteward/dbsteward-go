package pgsql8

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

var GlobalDb *Db = NewDB()

type Db struct {
	conn       *pgxpool.Pool
	activeRows []pgx.Rows
}

// TODO(go,nth) should this be in lib?
type DbResult struct {
	rows pgx.Rows
	db   *Db
}

// NOTE: This closes the result, cannot call Next() or FetchRowStringMap() after it's called
func (self *DbResult) RowCount() int {
	self.close()
	return int(self.rows.CommandTag().RowsAffected())
}
func (self *DbResult) Next() bool {
	v := self.rows.Next()
	if !v {
		self.close()
	}
	return v
}
func (self *DbResult) Err() error {
	return self.rows.Err()
}
func (self *DbResult) FetchRowStringMap() (map[string]string, error) {
	fields := self.rows.FieldDescriptions()
	cols := make([]string, len(fields))
	vals := make([]sql.NullString, len(fields))
	dests := make([]interface{}, len(fields))
	for i, field := range fields {
		cols[i] = string(field.Name)
		dests[i] = &vals[i]
	}
	err := self.rows.Scan(dests...)
	if err != nil {
		return nil, err
	}

	out := map[string]string{}
	for i, col := range cols {
		out[col] = vals[i].String
	}
	return out, nil
}

func (self *DbResult) close() {
	self.rows.Close()
	self.db.releaseRows(self.rows)
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
	// conn.Close will deadlock if there are any open rows, so close those first
	for _, rows := range self.activeRows {
		rows.Close()
	}
	self.conn.Close()
}

func (self *Db) Query(sql string, params ...interface{}) *DbResult {
	// "If there is an error the returned Rows will be returned in an error state.
	// So it is allowed to ignore the error returned from Query and handle it in Rows."
	rows, _ := self.conn.Query(context.TODO(), sql, params...)
	self.activeRows = append(self.activeRows, rows)
	return &DbResult{rows, self}
}

func (self *Db) QueryVal(val interface{}, sql string, params ...interface{}) error {
	return self.conn.QueryRow(context.TODO(), sql, params...).Scan(val)
}

func (self *Db) QueryStringMap(sql string, params ...interface{}) (map[string]string, error) {
	res := self.Query(sql, params...)
	if res.Next() {
		return res.FetchRowStringMap()
	}
	return nil, res.Err()
}

func (self *Db) releaseRows(rows pgx.Rows) {
	b := self.activeRows[:0]
	for _, r := range self.activeRows {
		if r != rows {
			b = append(b)
		}
	}
	for i := len(b); i < len(self.activeRows); i += 1 {
		self.activeRows[i] = nil
	}
	self.activeRows = b
}
