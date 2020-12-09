package lib

import (
	"github.com/dbsteward/dbsteward/lib/model"
)

var GlobalDBX *DBX = NewDBX()

type DBX struct {
}

func NewDBX() *DBX {
	return &DBX{}
}

func (self *DBX) SetDefaultSchema(def *model.Definition, schema string) {
	// TODO(go,core) dbx::set_default_schema()
}

func (self *DBX) BuildStagedSql(doc *model.Definition, ofs OutputFileSegmenter, stage int) {
	// TODO(go,core) dbx::build_staged_sql()
}
