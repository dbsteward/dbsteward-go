package pgsql8

var GlobalDb *Db = NewDB()

type Db struct {
}

// TODO(go,nth) should this be in lib?
type DbResult interface {
	RowCount() int
	Next() bool
	Err() error
	FetchRowStringMap() map[string]string // TODO(go,pgsql8) error handling
}

func NewDB() *Db {
	return &Db{}
}

func (self *Db) Connect(host string, port uint, name, user, pass string) {
	// TODO(go,pgsql)
}

func (self *Db) Disconnect() {
	// TODO(go,pgsql)
}

func (self *Db) Query(sql string, params ...interface{}) DbResult {
	// TODO(go,pgsql)
	return nil
}

func (self *Db) QueryVal(val interface{}, sql string, params ...interface{}) error {
	// TODO(go,pgsql)
	return nil
}

func (self *Db) QueryStringMap(sql string, params ...interface{}) (map[string]string, error) {
	var m map[string]string
	err := self.QueryVal(&m, sql, params...)
	return m, err
}
