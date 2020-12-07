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
	// TODO(go,core)
}
