package lib

import (
	"github.com/dbsteward/dbsteward/lib/model"
)

var GlobalDBX *DBX = NewDBX()

type DBX struct {
}

type ForeignColumnReference struct {
	Schema           *model.Schema
	Table            *model.Table
	Column           *model.Column
	Name             string
	ReferencesString string
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

func (self *DBX) ForeignKey(doc *model.Definition, schema *model.Schema, table *model.Table, column *model.Column) ForeignColumnReference {
	// TODO(go,core) dbx::foreign_ke
	// TODO(go,nth) should this live on model instead?
	return ForeignColumnReference{}
}
